"""
Class to map company descriptions to SIC codes.

    Usage:

    job_ads = {'id': 1, 'company_name': GreenJobs, 'job_text:' 'We are looking for a data scientist to join our team at Green Jobs.'}
    sm = SicMapper()
    sm.load() # load relevant models, tokenizers and datasets
    sic_codes = sm.get_sic_codes(job_ads) # get SIC codes for job adverts
"""
import faiss

import os
import yaml
from typing import List, Union, Dict, Optional

from tqdm import tqdm
import pandas as pd
import ast
import numpy as np

from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import pipeline

from dap_prinz_green_jobs import PROJECT_DIR, BUCKET_NAME, logger

# utils imports
from dap_prinz_green_jobs.utils.bert_vectorizer import BertVectorizer
import dap_prinz_green_jobs.utils.text_cleaning as tc
import dap_prinz_green_jobs.pipeline.green_measures.industries.sic_mapper.sic_mapper_utils as su
import dap_prinz_green_jobs.pipeline.green_measures.industries.industries_measures_utils as iu


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
        Preprocesses a job advert or list of job adverts to extract the company description.
    extract_company_descriptions(preprocessed_job_adverts):
        Extracts the company description from a list of job adverts.
    get_company_description_embeddings(company_description_dict):
        Gets the BERT embeddings for company descriptions.
    predict_sic_code(company_description):
        Predicts the SIC code for a company description.
    get_sic_codes(preprocessed_job_adverts):
        Predicts the SIC code for a list of preprocessed job adverts.
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
        if self.local:
            data_path_name = self.config["industries"]["data_path_name"]
            self.data_dir = os.path.join(PROJECT_DIR, data_path_name)
            logger.info(f"Loading data from {self.data_dir}/")
            if not os.path.exists(self.data_dir):
                logger.warning(
                    "Neccessary data files are not downloaded. Downloading neccessary files..."
                )
                os.system(f"aws s3 sync s3://{BUCKET_NAME}/data {self.data_dir}")
        else:
            self.data_dir = f"s3://{os.path.join(BUCKET_NAME, data_path_name)}"
            logger.info(f"Loading data from open {BUCKET_NAME} s3 bucket.")
        self.save_updated_outputs = self.config["industries"]["save_updated_outputs"]
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
        self.sic_db = self.config["industries"]["sic_db"]
        # load relevant information for BertVectorizer
        self.multi_process = self.config["industries"]["multi_process"]
        self.bert_model_name = self.config["industries"]["bert_model_name"]
        self.bert_model = BertVectorizer(
            multi_process=self.multi_process,
            bert_model_name=f"sentence-transformers/{self.bert_model_name}",
        ).fit()
        # load pre-defined company name hash to SIC codes, company description hash to embeddings
        self.comp_sic_mapper_path = self.config["industries"].get(
            "comp_sic_mapper_path"
        )
        self.comp_desc_emb_path = self.config["industries"].get("comp_desc_emb_path")

    def load(
        self,
        comp_sic_mapper_path: str = Optional[None],
        comp_desc_emb_path: str = Optional[None],
    ):
        """
        Loads relevant models, tokenizers and datasets.
        """
        # things that will make things faster - company name to SIC code mapper, comp desc hash to embeddings
        if (not comp_sic_mapper_path) and (self.comp_sic_mapper_path):
            comp_sic_mapper_path = self.comp_sic_mapper_path
        if (not comp_desc_emb_path) and (self.comp_desc_emb_path):
            comp_desc_emb_path = self.comp_desc_emb_path

        self.full_comp_sic_mapper_path = os.path.join(
            self.data_dir, self.comp_sic_mapper_path
        )
        self.comp_sic_mapper = pd.read_csv(self.full_comp_sic_mapper_path)

        self.full_comp_desc_emb_path = os.path.join(
            self.data_dir, self.comp_desc_emb_path
        )
        self.comp_desc_emb_mapper = pd.read_csv(self.full_comp_desc_emb_path)

        # things you need to load
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_path)
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_path)
        self.company_description_classifier = pipeline(
            "text-classification", model=self.model, tokenizer=self.tokenizer
        )

        full_sic_company_desc_path = os.path.join(
            self.data_dir, self.sic_comp_desc_path
        )
        self.sic_company_desc_dict = (
            pd.read_csv(full_sic_company_desc_path)
            .assign(sic_code=lambda x: x.sic_code.apply(ast.literal_eval))
            .to_dict("records")
        )

        # load your FAISS SIC company description index
        full_sic_db_path = os.path.join(self.data_dir, self.sic_db)
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
        preprocessed_job_adverts = []
        for job_advert in job_adverts:
            job_description_clean = tc.clean_text(job_advert[self.job_description_key])
            job_description_sentences = tc.split_into_sentences(job_description_clean)
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
                        company_description += sentence + ". "

            company_descriptions.append(
                {
                    self.job_id_key: job_advert[self.job_id_key],
                    self.company_name_key: job_advert[self.company_name_key],
                    "company_description": company_description,
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
        self.comp_desc_emb_mapper = (
            self.comp_desc_emb_mapper
            if isinstance(self.comp_desc_emb_mapper, dict)
            else {}
        )
        comps_to_embed = {
            hash: desc
            for hash, desc in company_descriptions_dict.items()
            if not self.comp_desc_emb_mapper.get(hash)
        }

        logger.info(
            f"Computing embeddings for {len(len(company_descriptions_dict) - len(comps_to_embed))} company descriptions..."
        )

        comp_embeds = self.bert_model.transform(list(comps_to_embed.values()))
        self.comp_desc_emb_mapper.update(
            dict(zip(list(comps_to_embed.keys()), comp_embeds))
        )

    def predict_sic_code(self, company_embedding: np.ndarray) -> List[int]:
        """Predicts the SIC code for a company description.

        Args:
            company_embedding (str): An embedding of a company description.

        Returns:
            List[int]: The predicted SIC code(s).
        """
        D, I = self.sic_db.search(company_embedding, self.k)  # search

        closest_distance = D[0][0]
        top_k_indices = I[0]

        # convert to similarity score
        sim_score = su.convert_faiss_distance_to_score(closest_distance)

        if sim_score < self.sim_threshold:
            sic_code_indx = top_k_indices[0]
            sics = self.sic_company_desc_dict[sic_code_indx]["sic_code"]
            sic_code = [code.strip() for code in sics]

        else:
            # get majority sic based on standard deviation of k
            sic_code = su.find_majority_sic(top_k_indices)

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
        if self.comp_sic_mapper:
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

        self.get_company_description_embeddings(
            company_description_dict
        )  # update comp_desc_emb_mapper
        sic_codes = []
        for job_ad in preprocessed_job_adverts_comp_desc:
            company_name = job_ad.get(self.company_name_key)
            if comp_sic_mapper_filtered:
                sic_code = comp_sic_mapper_filtered.get(company_name)
            else:
                company_desc_hash = tc.short_hash(job_ad["company_description"])
                comp_embed = self.comp_desc_emb_mapper.get(company_desc_hash)
                sic_code = self.predict_sic_code(np.array(comp_embed))
            sic_codes.append(
                {
                    self.job_id_key: job_ad[self.job_id_key],
                    self.company_name_key: company_name,
                    self.job_description_key: job_ad[
                        f"{self.job_description_key}_clean"
                    ],
                    "company_description": job_ad["company_description"],
                    "SIC code": sic_code,
                }
            )

        return sic_codes
