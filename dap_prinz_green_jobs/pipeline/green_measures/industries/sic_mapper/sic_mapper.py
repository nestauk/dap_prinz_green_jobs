"""
Class to extract company descriptions and map them to up to a SIC code
    at different levels.

Usage:

    job_ads = {'id': 1,
    'job_text': 'We are looking for a software engineer to join our team. We are a fast growing company in the software engineering industry.'}

    sm = SicMapper()
    sm.load() # load relevant models, tokenizers and datasets
    sic_code = sm.get_sic_codes(job_ads) # get SIC codes for job adverts

  >>  [{'id': '1',
      'company_description': 'We are a fast growing company in the software engineering industry.',
      'sic_code': '582',
      'sic_name': 'Software publishing',
      'sic_method': 'closest distance',
      'sic_confidence': 0.77}]
"""
import faiss

import os
import yaml
from typing import List, Union, Dict

from tqdm import tqdm
import numpy as np

from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import pipeline
import torch

from dap_prinz_green_jobs import PROJECT_DIR, BUCKET_NAME, logger
from dap_prinz_green_jobs.getters.data_getters import load_s3_data, load_json_dict
from dap_prinz_green_jobs.getters.industry_getters import load_sic

# utils imports
from dap_prinz_green_jobs.utils.bert_vectorizer import BertVectorizer
import dap_prinz_green_jobs.utils.text_cleaning as tc
import dap_prinz_green_jobs.pipeline.green_measures.industries.sic_mapper.sic_mapper_utils as su

from toolz import partition_all


class SicMapper(object):
    """
    Class to extract company descriptions from job adverts and map them to SIC codes.
    ----------
    Parameters
    ----------
    config_name: str
        Name of the config file to use. Default is "base.yaml".
    ----------
    Methods
    ----------
    load():
        Loads relevant models, tokenizers and datasets necessary for the SicMapper class.
    preprocess_job_advert(job_advert):
        Preprocesses a list of job adverts to extract the company description.
    extract_company_descriptions(preprocessed_job_adverts):
        Extracts the company description from a list of job adverts.
    get_company_description_embeddings(company_description_dict):
        Gets the BERT embeddings for company descriptions.
    create_vector_index(sic_embeds):
        Creates a FAISS index for the SIC company description embeddings.
    predict_sic_code(company_description):
        Predicts the SIC code for a company description.
    get_sic_codes(preprocessed_job_adverts):
        Predicts the SIC code for a job advert or list of preprocessed job adverts.
    ----------
    """

    def __init__(
        self, config_name: str = "base", use_gpu: bool = False, chunk_size: int = 100
    ):
        # Set variables from the config file
        if ".yaml" not in config_name:
            config_name += ".yaml"
        config_path = os.path.join(
            PROJECT_DIR, "dap_prinz_green_jobs/config/", config_name
        )
        with open(config_path, "r") as f:
            self.config = yaml.load(f, Loader=yaml.FullLoader)
        self.config_path = config_path
        self.verbose = self.config["industries"]["verbose"]
        self.use_gpu = use_gpu
        self.chunk_size = chunk_size

        if self.verbose:
            logger.setLevel("INFO")
        else:
            logger.setLevel("ERROR")
        self.local = self.config["industries"]["local"]
        self.data_path = self.config["job_adverts"]["data_folder_name"]
        # for now self.local downloads ALL files in the green_industries folder
        # in a private bucket - we could change this later for people who don't access
        # to the bucket and want to use SicMapper
        if self.local:
            self.data_dir = os.path.join(
                PROJECT_DIR, self.data_path, "green_industries"
            )
            logger.info(f"Loading data from {self.data_dir}/")
            if not os.path.exists(self.data_dir):
                logger.warning(
                    "Neccessary data files are not downloaded. Downloading neccessary files..."
                )
                os.system(
                    f"aws s3 sync s3://{BUCKET_NAME}/data {self.data_dir}/green_industries"
                )
            else:
                # count the number of files in the directory to check if all files are there
                file_num = len(
                    [
                        name
                        for name in os.listdir(self.data_dir)
                        if os.path.isfile(os.path.join(self.data_dir, name))
                    ]
                )
                if file_num < 5:
                    logger.warning(
                        "Neccessary data files are not downloaded. Downloading neccessary files..."
                    )
                    os.system(
                        f"aws s3 sync s3://{BUCKET_NAME}/outputs/data/green_industries {self.data_dir}"
                    )
        else:
            self.data_dir = os.path.join(self.data_path, "green_industries")
            logger.info(f"Loading data from open {BUCKET_NAME} s3 bucket.")
        # get job id and job description keys
        self.job_id_key = self.config["job_adverts"]["job_id_key"]
        self.job_description_key = self.config["job_adverts"]["job_text_key"]
        # load company description classifier model name
        self.model_path = self.config["industries"]["model_path"]
        # load relevant information to map company descriptions to SIC codes
        self.sic_comp_desc_path = self.config["industries"]["sic_comp_desc_path"]
        # binary of whether to use companies house data or not as part of the sic
        self.faiss_k = self.config["industries"]["faiss_k"]
        self.closest_distance_threshold = self.config["industries"][
            "closest_distance_threshold"
        ]
        self.majority_sic_threshold = self.config["industries"][
            "majority_sic_threshold"
        ]
        self.sic_levels = self.config["industries"]["sic_levels"]
        self.sic_comp_desc_embeds_path = self.config["industries"][
            "sic_comp_desc_embeds_path"
        ]
        # load relevant information for BertVectorizer
        self.multi_process = self.config["industries"]["multi_process"]
        self.bert_model_name = self.config["industries"]["bert_model_name"]
        self.bert_model = BertVectorizer(
            multi_process=self.multi_process,
            bert_model_name=f"sentence-transformers/{self.bert_model_name}",
        ).fit()

    def _fetch_data(self, file_name: str = None):
        """Wrapper to fetch data from local or s3.

        Args:
            data_path (str, optional): Path to load data. Defaults to None.

        Returns:
            Data in the form of a dictionary.
        """
        full_path = os.path.join(self.data_dir, file_name)
        if file_name.endswith(".json"):
            if self.local:
                data = load_json_dict(full_path)
            else:
                data = load_s3_data(BUCKET_NAME, full_path)
            return data
        else:
            assert file_name.endswith(".json"), "File type not supported."
            logger.error("Data path must be a .json file.")

    def load(self):
        """
        Loads relevant models, tokenizers and datasets.
        """
        logger.info("Loading relevant models, tokenizers and datasets.")
        # things you need to load
        tokenizer = AutoTokenizer.from_pretrained(self.model_path)

        device = torch.device(f"cuda:0" if torch.cuda.is_available() else "cpu")
        model = AutoModelForSequenceClassification.from_pretrained(self.model_path).to(
            device
        )
        self.company_description_classifier = pipeline(
            "text-classification", model=model, tokenizer=tokenizer, device=device
        )

        self.sic_company_desc_dict = self._fetch_data(self.sic_comp_desc_path)
        # load your SIC company description embeddings to put in a FAISS index
        sic_embeds = self._fetch_data(self.sic_comp_desc_embeds_path)
        self.sic_comp_desc_embeds = np.float32(np.array(sic_embeds))

        self.sic_db = self.create_vector_index(self.sic_comp_desc_embeds)

        sic_data = load_sic()
        self.sic_names = dict(
            zip(sic_data["Most disaggregated level"], sic_data["Description"])
        )
        self.sic_names = {str(k).strip(): v for k, v in self.sic_names.items()}

        self.sic_to_section = {
            str(k).strip(): v.strip()
            for k, v in dict(
                zip(sic_data["Most disaggregated level"], sic_data["SECTION"])
            ).items()
        }

        # based on evaluation, we have hard coded for some companies
        self.hard_coded_sics = su.hard_coded_sics

    def preprocess_job_adverts(
        self, job_adverts: List[Dict[str, str]]
    ) -> List[Dict[str, str]]:
        """Preprocesses a list of job adverts to extract the company description.

        Pre-processing includes:
            - minor camel case formatting;
            - splitting job advert into sentences.

        Args:
            job_adverts (List[Dict[str, str]]]): A list of job adverts.

        Returns:
            List[Dict[str, str]]: A job advert with a pre-processed job description.
        """
        logger.info(f"preprocessing {len(job_adverts)} job adverts...")
        preprocessed_job_adverts = []
        for job_advert in job_adverts:
            job_description_clean = tc.clean_text(job_advert[self.job_description_key])
            job_description_sentences = tc.split_sentences(job_description_clean)
            preprocessed_job_adverts.append(
                {
                    self.job_id_key: job_advert[self.job_id_key],
                    f"{self.job_description_key}_clean": job_description_clean,
                    f"{self.job_description_key}_sentences": job_description_sentences,
                }
            )
        return preprocessed_job_adverts

    def extract_company_descriptions(
        self, preprocessed_job_adverts: List[Dict[str, str]], chunk_size=100
    ) -> List[Dict[str, str]]:
        """
        Extracts the company description from a list of job adverts.

        Args:
            preprocessed_job_adverts (List[Dict[str, str]]): A list of job adverts with pre-processed job descriptions.

        Returns:
            List[Dict[str, str]]: A list of job adverts with the company description.
        """
        logger.info(
            f"Extracting company descriptions from {len(preprocessed_job_adverts)} job adverts..."
        )

        # Get all the individual sentences per job advert
        job_ad_sents = {}
        for job_advert in preprocessed_job_adverts:
            sents = []
            for sentence in job_advert[f"{self.job_description_key}_sentences"]:
                if (
                    10 < len(sentence) < 300
                ):  # Only predict on reasonable length sentences
                    sents.append(sentence)
            job_ad_sents[job_advert[self.job_id_key]] = sents

        # Get the unique sentences
        all_sents = [v for s in job_ad_sents.values() for v in s]
        logger.info(f"{len(all_sents)} sentences...")
        all_sents = list(set(all_sents))
        logger.info(
            f"...deduplicated to {len(all_sents)} unique sentences for processing ..."
        )

        # Predict whether a sentence is a company description
        sent_is_company = set()
        if self.use_gpu:
            preds = self.company_description_classifier(
                all_sents, batch_size=chunk_size
            )
            for pred, sentence in zip(preds, all_sents):
                if pred["label"] == "LABEL_1":
                    sent_is_company.add(sentence)
        else:
            all_sents_chunks = list(partition_all(chunk_size, all_sents))
            for sents_chunk in all_sents_chunks:
                preds = self.company_description_classifier(list(sents_chunk))
                for pred, sentence in zip(preds, sents_chunk):
                    if pred["label"] == "LABEL_1":
                        sent_is_company.add(sentence)

        # Join all the information together
        company_descriptions = []
        for job_advert in preprocessed_job_adverts:
            company_description = ""
            job_advert_id = job_advert[self.job_id_key]
            # Retrieve the sentences for this job advert
            job_sents = job_ad_sents[job_advert_id]
            for sent in job_sents:
                if sent in sent_is_company:
                    company_description += f"{sent}. "
            company_description_clean = su.clean_company_description(
                company_description
            )
            company_descriptions.append(
                {
                    self.job_id_key: job_advert[self.job_id_key],
                    f"{self.job_description_key}_clean": job_advert.get(
                        f"{self.job_description_key}_clean"
                    ),
                    "company_description": company_description_clean,
                }
            )

        return company_descriptions

    def get_company_description_embeddings(
        self, company_descriptions_dict: Dict[str, str]
    ) -> Dict[str, str]:
        """Gets company description embeddings.

        Args:
            company_descriptions_dict (Dict[str, str]): A dictionary
                of company description hashes and company descriptions.
        """
        logger.info(
            f"Computing embeddings for {len(company_descriptions_dict)} company descriptions..."
        )

        comp_embeds = self.bert_model.transform(
            list(company_descriptions_dict.values())
        )
        comp_embeds_dict = dict(
            zip(list(company_descriptions_dict.keys()), comp_embeds)
        )

        return comp_embeds_dict

    def create_vector_index(self, sic_embeddings: np.ndarray) -> faiss.IndexFlatIP:
        """Creates a FAISS index for a given set of embeddings.

        Args:
            embeddings (np.ndarray): Embeddings to put in the index.

        Returns:
            faiss.IndexFlatIP: A FAISS index.
        """
        logger.info(f"Creating FAISS index for vectors...")
        d = sic_embeddings.shape[1]  # define the dimensionality of the vectors
        # lets use brute force L2
        llm_index = faiss.IndexFlatIP(d)
        # faiss.normalize_L2(sic_embeddings)  # normalise to use cosine distance
        llm_index.add(sic_embeddings)  # add vectors to the index

        return llm_index

    def predict_sic_code(self, company_embedding: np.ndarray) -> Union[str, None]:
        """Predicts the majority SIC code at a given level for a company description embedding.

        Args:
            company_embedding (str): An embedding of a company description.

        Returns:
            Union[str, None]: The predicted SIC code or None if not found.
        """
        _vector = np.array([company_embedding])
        faiss.normalize_L2(_vector)  # normalise to use cosine distance

        D, I = self.sic_db.search(_vector, self.faiss_k)  # search

        closest_distance = D[0][0]
        top_k_indices, top_k_distances = I[0], D[0]

        if closest_distance > self.closest_distance_threshold:
            sic_code_indx, sic_prob = top_k_indices[0], round(top_k_distances[0], 2)
            sics = self.sic_company_desc_dict[sic_code_indx]["sic_code"]
            sic_code = [str(code).strip() for code in sics][0]
            sic_name = self.sic_names.get(sic_code)
            sic_method = "closest distance"

        else:
            std = np.std(D)
            std_threshold = closest_distance + 2 * std  # Use a std threshold
            top_dists = [d for d in top_k_distances if d < std_threshold]
            top_sics = su.convert_indx_to_sic(
                top_k_indices[: len(top_dists)], self.sic_company_desc_dict
            )

            # find the majority sic at sic levels 2-,3- and 4-
            top_candidate_sics = [
                su.find_majority_sic(top_sics, lvl) for lvl in self.sic_levels
            ]
            all_maj_sics = {k: v for d in top_candidate_sics for k, v in d.items()}
            sic_method = "majority SIC"

            if all_maj_sics != {}:
                sic_code = sorted(
                    all_maj_sics.items(), key=lambda x: x[1], reverse=True
                )[0][0]

                # calculate the average distance of the top majority SIC
                # make sure its above the majority SIC threshold
                sic_prob = su.calculate_average_distance(sic_code, top_sics, top_dists)
                sic_name = self.sic_names.get(sic_code)
                if sic_prob < self.majority_sic_threshold:
                    sic_code = None
                    sic_prob = None
                    sic_method = None
                    sic_name = None

            else:
                return None, None, None, None

        return sic_code, sic_prob, sic_method, sic_name

    def get_sic_codes(self, job_adverts: Union[Dict[str, str], List[Dict[str, str]]]):
        """Finds the SIC code for a job advert or list of job adverts.

        Args:
            job_adverts (Union[Dict[str, str], List[Dict[str, str]]]): A job advert or list of job adverts.

        Returns:
            List[int]: The predicted SIC code(s) associated to a job advert or list of job adverts.
        """

        if isinstance(job_adverts, dict):
            job_adverts = [job_adverts]

        sic_codes = {}
        job_ids_to_predict = []
        # lets first hard code sic codes  if part of the company description is in `self.hard_coded_sics`
        for job_ad in job_adverts:
            job_ad_text = job_ad.get(self.job_description_key)
            if job_ad_text:
                sic_code = [
                    v for k, v in self.hard_coded_sics.items() if k in job_ad_text
                ]
                if sic_code != []:
                    sic_codes[job_ad.get(self.job_id_key)] = {
                        "company_description": None,
                        "sic_code": sic_code[0],
                        "sic_name": self.sic_names.get(sic_code[0]),
                        "sic_method": "hard coded sic",
                        "sic_confidence": None,
                    }
                else:
                    job_ids_to_predict.append(job_ad.get(self.job_id_key))

        if len(job_ids_to_predict) > 0:
            logger.info(
                f"{len(job_ids_to_predict)} job adverts don't have SIC codes associated to them in companies house..."
            )
            logger.info(
                f"predicting SIC code for {len(job_ids_to_predict)} job adverts..."
            )

            job_adverts = [
                advert
                for advert in job_adverts
                if str(advert["id"]) in map(str, job_ids_to_predict)
            ]

            preprocessed_job_adverts = self.preprocess_job_adverts(job_adverts)

            preprocessed_job_adverts_comp_desc = self.extract_company_descriptions(
                preprocessed_job_adverts
            )

            company_descriptions = list(
                set(
                    [
                        job_advert["company_description"]
                        for job_advert in preprocessed_job_adverts_comp_desc
                    ]
                )
            )
            company_description_hashes = [
                tc.short_hash(company_description)
                for company_description in company_descriptions
            ]

            company_description_dict = dict(
                zip(company_description_hashes, company_descriptions)
            )

            comp_embeds = self.get_company_description_embeddings(
                company_description_dict
            )

            for job_ad in preprocessed_job_adverts_comp_desc:
                if job_ad["company_description"] != "":
                    company_desc_hash = tc.short_hash(job_ad["company_description"])
                    comp_embed = comp_embeds.get(company_desc_hash)
                    sic_code, sic_prob, sic_method, sic_name = self.predict_sic_code(
                        np.array(comp_embed)
                    )
                else:
                    sic_code, sic_prob, sic_method, sic_name = None, None, None, None
                sic_codes[job_ad[self.job_id_key]] = {
                    "company_description": job_ad["company_description"],
                    "sic_code": sic_code,
                    "sic_name": sic_name,
                    "sic_method": sic_method,
                    "sic_confidence": sic_prob,
                }

        return sic_codes
