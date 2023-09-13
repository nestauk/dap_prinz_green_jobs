"""
Class to extract company descriptions and map them to up to a 4-digit SIC code.

Usage:

    job_ads = {'id': 1, 'company_name': Google, 'job_text:' 'We are looking for a software engineer to join our team. We are a fast growing company in the software engineering industry.'}
    sm = SicMapper()
    sm.load() # load relevant models, tokenizers and datasets
    sic_code = sm.get_sic_code(job_ads) # get SIC codes for job adverts

  >>  [{'id': '1',
      'company_name': 'Google',
      'job_text': 'We are looking for a software engineer to join our team. We are a fast growing company in the software engineering industry.',
      'company_description': 'We are a fast growing company in the software engineering industry.',
      'sic_code': '582'}]
"""
import faiss

import os
import yaml
from typing import List, Union, Dict

from tqdm import tqdm
import pandas as pd
import ast
import numpy as np

from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import pipeline

from dap_prinz_green_jobs import PROJECT_DIR, BUCKET_NAME, logger
from dap_prinz_green_jobs.getters.data_getters import load_s3_data, load_json_dict

# utils imports
from dap_prinz_green_jobs.utils.bert_vectorizer import BertVectorizer
import dap_prinz_green_jobs.utils.text_cleaning as tc
import dap_prinz_green_jobs.pipeline.green_measures.industries.sic_mapper.sic_mapper_utils as su


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
    predict_sic_code(company_description):
        Predicts the SIC code for a company description.
    get_sic_codes(preprocessed_job_adverts):
        Predicts the SIC code for a job advert or list of preprocessed job adverts.
    ----------
    """

    def __init__(
        self,
        config_name: str = "base",
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

        if self.verbose:
            logger.setLevel("INFO")
        else:
            logger.setLevel("ERROR")
        self.local = self.config["industries"]["local"]
        self.data_path = self.config["job_adverts"]["data_folder_name"]
        # for now self.local downloads ALL files in the green_industries folder
        # in a private bucket - we will change this later
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
        self.company_name_key = self.config["job_adverts"]["company_name_key"]
        # load company description classifier model name
        self.model_path = self.config["industries"]["model_path"]
        # load relevant information to map company descriptions to SIC codes
        self.sic_comp_desc_path = self.config["industries"]["sic_comp_desc_path"]
        self.k = self.config["industries"]["k"]
        self.sim_threshold = self.config["industries"]["sim_threshold"]
        self.sic_levels = self.config["industries"]["sic_levels"]
        self.sic_db = self.config["industries"]["sic_db"]
        # load relevant information for BertVectorizer
        self.multi_process = self.config["industries"]["multi_process"]
        self.bert_model_name = self.config["industries"]["bert_model_name"]
        self.bert_model = BertVectorizer(
            multi_process=self.multi_process,
            bert_model_name=f"sentence-transformers/{self.bert_model_name}",
        ).fit()
        # load pre-defined company name hash to SIC codes, company description hash to embeddings
        # these files don't exist right now but would be created to make the pipeline more efficient
        self.comp_sic_mapper_path = self.config["industries"].get(
            "comp_sic_mapper_path"
        )
        self.comp_desc_emb_path = self.config["industries"].get("comp_desc_emb_path")

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

    def load(
        self,
        comp_sic_mapper_path: str = None,
        comp_desc_emb_path: str = None,
    ):
        """
        Loads relevant models, tokenizers and datasets.
        """
        logger.info("Loading relevant models, tokenizers and datasets.")
        # things that will make things faster - company name to SIC code mapper, comp desc hash to embeddings
        if (not comp_sic_mapper_path) and (self.comp_sic_mapper_path):
            comp_sic_mapper_path = self.comp_sic_mapper_path
        if (not comp_desc_emb_path) and (self.comp_desc_emb_path):
            comp_desc_emb_path = self.comp_desc_emb_path

        self.comp_sic_mapper = (
            self._fetch_data(self.full_comp_sic_mapper_path)
            if comp_sic_mapper_path
            else {}
        )
        self.comp_desc_emb_mapper = (
            self._fetch_data(self.full_comp_desc_emb_path) if comp_desc_emb_path else {}
        )

        # things you need to load
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_path)
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_path)
        self.company_description_classifier = pipeline(
            "text-classification", model=self.model, tokenizer=self.tokenizer
        )
        self.sic_company_desc_dict = self._fetch_data(self.sic_comp_desc_path)

        # load your FAISS SIC company description index - this must be local for now
        # we will need to change this eventually
        full_sic_db_path = os.path.join(
            PROJECT_DIR, self.data_path, "green_industries", self.sic_db
        )
        self.sic_db = faiss.read_index(full_sic_db_path)

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
                    self.company_name_key: job_advert[self.company_name_key],
                    f"{self.job_description_key}_clean": job_description_clean,
                    f"{self.job_description_key}_sentences": job_description_sentences,
                }
            )
        return preprocessed_job_adverts

    def extract_company_descriptions(
        self, preprocessed_job_adverts: List[Dict[str, str]]
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
        company_descriptions = []
        for job_advert in tqdm(preprocessed_job_adverts):
            company_description = ""
            for sentence in job_advert[f"{self.job_description_key}_sentences"]:
                if (
                    10 < len(sentence) < 250
                ):  # Only predict on reasonable length sentences
                    pred = self.company_description_classifier(sentence)[0]
                    if pred["label"] == "LABEL_1":
                        company_description += f"{sentence}. "

            company_descriptions.append(
                {
                    self.job_id_key: job_advert[self.job_id_key],
                    self.company_name_key: job_advert[self.company_name_key],
                    f"{self.job_description_key}_clean": job_advert.get(
                        f"{self.job_description_key}_clean"
                    ),
                    "company_description": company_description.strip(),
                }
            )

        return company_descriptions

    def get_company_description_embeddings(
        self, company_descriptions_dict: Dict[str, str]
    ) -> Dict[str, str]:
        """Updates the comp_desc_emb_mapper attribute with company description embeddings.

        Args:
            company_descriptions_dict (Dict[str, str]): A dictionary
                of company description hashes and company descriptions.
        """
        comps_to_embed = {
            hash: desc
            for hash, desc in company_descriptions_dict.items()
            if not self.comp_desc_emb_mapper.get(hash)
        }
        logger.info(
            f"Computing embeddings for {len(comps_to_embed)} company descriptions..."
        )

        comp_embeds = self.bert_model.transform(list(comps_to_embed.values()))
        comp_embeds_dict = dict(zip(list(comps_to_embed.keys()), comp_embeds))

        return comp_embeds_dict

    def predict_sic_code(self, company_embedding: np.ndarray) -> Union[str, None]:
        """Predicts the majority SIC code at a given level for a company description embedding.

        Args:
            company_embedding (str): An embedding of a company description.

        Returns:
            Union[str, None]: The predicted SIC code or None if not found.
        """

        _vector = np.array([company_embedding])
        faiss.normalize_L2(_vector)

        D, I = self.sic_db.search(_vector, self.k)  # search

        closest_distance = D[0][0]
        top_k_indices, top_k_distances = I[0], D[0]

        if closest_distance > self.sim_threshold:
            sic_code_indx = top_k_indices[0]
            sics = self.sic_company_desc_dict[sic_code_indx]["sic_code"]
            sic_code = [str(code).strip() for code in sics][0]
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

            # for now, get the sic code with the highest count across levels
            sic_code = sorted(all_maj_sics.items(), key=lambda x: x[1], reverse=True)[
                0
            ][0]

        return sic_code

    def get_sic_codes(self, job_adverts: Union[Dict[str, str], List[Dict[str, str]]]):
        """Finds the SIC code for a job advert or list of job adverts.

        Args:
            job_adverts (Union[Dict[str, str], List[Dict[str, str]]]): A job advert or list of job adverts.

        Returns:
            List[int]: The predicted SIC code(s) associated to a job advert or list of job adverts.
        """
        if isinstance(job_adverts, dict):
            job_adverts = [job_adverts]

        # get company names first
        company_names = list(
            set([job_advert[self.company_name_key] for job_advert in job_adverts])
        )

        comp_sic_mapper_filtered = {
            comp_name: sic_code
            for comp_name, sic_code in self.comp_sic_mapper.items()
            if comp_name in company_names
        }
        job_adverts = [
            job_advert
            for job_advert in job_adverts
            if not comp_sic_mapper_filtered.get(job_advert[self.company_name_key])
        ]

        # now lets get company descriptions for the filtered job adverts
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

        comp_embeds = self.get_company_description_embeddings(company_description_dict)
        sic_codes = []
        for job_ad in preprocessed_job_adverts_comp_desc:
            company_name = job_ad.get(self.company_name_key)
            sic_code = comp_sic_mapper_filtered.get(company_name)
            if not sic_code:
                if job_ad["company_description"] != "":
                    company_desc_hash = tc.short_hash(job_ad["company_description"])
                    comp_embed = comp_embeds.get(company_desc_hash)
                    sic_code = self.predict_sic_code(np.array(comp_embed))
                else:
                    sic_code = None
            sic_codes.append(
                {
                    self.job_id_key: job_ad[self.job_id_key],
                    self.company_name_key: company_name,
                    self.job_description_key: job_ad[
                        f"{self.job_description_key}_clean"
                    ],
                    "company_description": job_ad["company_description"],
                    "sic_code": sic_code,
                }
            )

        return sic_codes
