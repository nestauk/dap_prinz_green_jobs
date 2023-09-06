from dap_prinz_green_jobs.utils.bert_vectorizer import get_embeddings
from dap_prinz_green_jobs import PROJECT_DIR, get_yaml_config, BUCKET_NAME, logger
from dap_prinz_green_jobs.getters.data_getters import save_to_s3, load_s3_data
from dap_prinz_green_jobs.utils.processing import list_chunks
from dap_prinz_green_jobs.pipeline.green_measures.skills.green_skill_classifier import (
    GreenSkillClassifier,
)

from ojd_daps_skills.pipeline.extract_skills.extract_skills import ExtractSkills
from ojd_daps_skills.pipeline.skill_ner.ner_spacy import JobNER
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import pandas as pd
from tqdm import tqdm

from typing import List, Dict, Tuple
from itertools import islice
import yaml
import os
from collections import defaultdict


def window_split(sentence, max_entity_size=10, window_overlap=5):
    """
    Split a sentence by a sliding window
    e.g. "This is a sentence about cats" with max_entity_size=4, window_overlap=2
    -> ['This is a sentence', 'a sentence about cats']
    """
    word_list = sentence.split()
    if len(word_list) <= max_entity_size:
        return [sentence]
    else:
        return [
            " ".join(word_list[i : i + max_entity_size])
            for i in range(0, len(word_list), max_entity_size - window_overlap)
            if len(word_list[i : i + max_entity_size]) > window_overlap
        ]


def split_up_skill_entities(
    entity: str, max_entity_size: int = 10, window_overlap: int = 5
) -> list:
    """
    Split up an entity into smaller chunks if it's long.
    Some predicted entities can be quite long, so mapping to a taxonomy can be hard.
    """

    split_entities = entity.split(";")
    # Remove blank "skills" and double spaces [" ", "This is a    sentence"] -> ["This is a sentence"]
    split_entities = [w for w in [" ".join(s.split()) for s in split_entities] if w]

    # Window split for long entities
    split_ent_list = []
    for entity_sentence in split_entities:
        entity_sentence_words = entity_sentence.split()
        split_sentences = window_split(
            entity_sentence,
            max_entity_size=max_entity_size,
            window_overlap=window_overlap,
        )
        for split_sentence in split_sentences:
            split_ent_list.append(split_sentence)

    return split_ent_list


class SkillMeasures(object):
    def __init__(self, config_name="extract_green_skills_esco"):
        self.config = get_yaml_config(
            PROJECT_DIR / f"dap_prinz_green_jobs/config/{config_name}.yaml"
        )
        self.skill_threshold = self.config["match_thresholds_dict"][
            "skill_match_thresh"
        ]

        formatted_taxonomy_path = self.config["taxonomy_path"]
        self.formatted_taxonomy = load_s3_data(BUCKET_NAME, formatted_taxonomy_path)

    def initiate_extract_skills(self, local=True, verbose=True):
        """
        Ideally the ojd_daps_skills package would be refactored to not need this step,
        but for now we need to be a bit hacky in order to use a custom config
        """
        # Base intansiation (but variables will be replaced with the input config though)
        es = ExtractSkills(
            config_name="extract_skills_toy", local=local, verbose=verbose
        )

        if es.local:
            es.s3 = False
            es.ner_model_path = os.path.join(PROJECT_DIR, self.config["ner_model_path"])
        else:
            es.s3 = True
            es.ner_model_path = os.path.join(
                "escoe_extension/", self.config["ner_model_path"]
            )

        es.labels = ["SKILL", "MULTISKILL", "EXPERIENCE", "BENEFIT"]

        # Load NER model
        es.job_ner = JobNER()
        es.nlp = es.job_ner.load_model(es.ner_model_path, s3_download=es.s3)

        # Other variables in ExtractSkills
        es.clean_job_ads = self.config["clean_job_ads"]
        es.min_multiskill_length = self.config["min_multiskill_length"]

        taxonomy_info_names = [
            "num_hier_levels",
            "skill_type_dict",
            "match_thresholds_dict",
            "hier_name_mapper",
            "skill_name_col",
            "skill_id_col",
            "skill_hier_info_col",
            "skill_type_col",
        ]
        es.taxonomy_info = {name: self.config.get(name) for name in taxonomy_info_names}
        self.es = es

    def get_entities(
        self,
        job_adverts: list,
        output_path: str = "",
        load: bool = False,
        job_text_key: str = "job_text",
        job_id_key: str = "id",
    ) -> dict:
        """
        Get entities for job adverts - whether by prediction or by loading existing predictions

        Args:
                job_adverts (list): The job advert texts e.g. [{"id": "abc", "text": "full job advert text"}, {..}]
                output_path (str): The output path if you want to save/load the predicted entities
                load (bool): If you want to load entities from output_path (True) or predict them again (False)
                job_text_key (str): the key for the job text
        Returns:
                dict: A dictionary of job advert ids to the predicted entities
        """

        if load:
            logger.info(f"Loading skills from {output_path}")
            predicted_skills = load_s3_data(
                BUCKET_NAME,
                output_path,
            )
        else:
            logger.info(f"Predicting skills for {len(job_adverts)} job adverts")

            predicted_skills = self.es.get_skills(
                [j[job_text_key] for j in job_adverts]
            )  # extract skills from list of job adverts
            predicted_skills = dict(
                zip([j[job_id_key] for j in job_adverts], predicted_skills)
            )

            if output_path:
                logger.info(f"Saving predicted skills to {output_path}")
                save_to_s3(
                    BUCKET_NAME,
                    predicted_skills,
                    output_path,
                )

        return predicted_skills

    def get_skill_embeddings(
        self, skills_list: list, output_path: str = "", load: bool = False
    ) -> dict:
        """
        Get skill embeddings for a list of skills - whether by calculation or by loading existing embeddings

        Args:
                        skills_list (list): A list of skills, e.g. ["communication", "excel skills"]
                        output_path (str): The output path if you want to save/load the embeddings
                        load (bool): If you want to load embeddings from output_path (True) or create them again (False)
        Returns:
                        dict: A dictionary of skills to their embeddings
        """

        if load and not output_path:
            logger.info(f"You need to specifiy a path to load from in output_path")

        if load:
            logger.info(f"Loading skill embeddings from {output_path}")
            self.all_extracted_skills_embeddings_dict = load_s3_data(
                BUCKET_NAME,
                output_path,
            )
        else:
            logger.info(f"Calculating skill embeddings for {len(skills_list)} skills")

            self.all_extracted_skills_embeddings_dict = get_embeddings(skills_list)

            if output_path:
                logger.info(f"Saving skill embeddings to {output_path}")
                save_to_s3(
                    BUCKET_NAME,
                    self.all_extracted_skills_embeddings_dict,
                    output_path,
                )

        return self.all_extracted_skills_embeddings_dict

    def get_green_taxonomy_embeddings(
        self, output_path: str = "", load: bool = False
    ) -> dict:
        """
        Get taxonomy embeddings - whether by calculation or by loading existing embeddings

        Args:
                        output_path (str): The output path if you want to save/load the embeddings
                        load (bool): If you want to load embeddings from output_path (True) or create them again (False)
        Returns:
                        dict: A dictionary of taxonomy skills to their embeddings
        """

        if load and not output_path:
            logger.info(f"You need to specify a path to load from in output_path")

        if load:
            logger.info(f"Loading taxonomy embeddings from {output_path}")
            self.taxonomy_skills_embeddings_dict = load_s3_data(
                BUCKET_NAME,
                output_path,
            )
        else:
            logger.info(
                f"Calculating embeddings for {len(self.formatted_taxonomy)} taxonomy skills"
            )

            self.taxonomy_skills_embeddings_dict = get_embeddings(
                self.formatted_taxonomy["description"].to_list(),
                id_list=list(self.formatted_taxonomy.index),
            )

            if output_path:
                logger.info(f"Saving taxonomy embeddings to {output_path}")
                save_to_s3(
                    BUCKET_NAME, self.taxonomy_skills_embeddings_dict, output_path
                )

        return self.taxonomy_skills_embeddings_dict

    def map_green_skills(
        self, skill_ents: list, all_extracted_skills_embeddings_dict: dict
    ) -> dict:
        """
        Use a trained classifier to find out whether extracted skills are likely to be green or not,
        and map them to the most semantically similar green ESCO skill if so

        Args:
            skill_ents: a list of skills
            all_extracted_skills_embeddings_dict: the associated embeddings for the skills in skill_ents

        Returns:
            dict: The skill and green skill information:
                 [[green/not-green, probability of this prediction, closest green ESCO skill]]

        """

        green_skills_classifier = GreenSkillClassifier()
        green_skills_classifier.taxonomy_skills_embeddings_dict = (
            self.taxonomy_skills_embeddings_dict
        )
        green_skills_classifier.formatted_taxonomy = self.formatted_taxonomy

        green_skills_classifier.load(
            model_file="s3://prinz-green-jobs/outputs/models/green_skill_classifier/green_skill_classifier_20230906.joblib"
        )

        pred_green_skill = green_skills_classifier.predict(
            skill_ents, skills_list_embeddings_dict=all_extracted_skills_embeddings_dict
        )

        return dict(zip(skill_ents, pred_green_skill))

    def get_measures(
        self,
        ents_per_job: defaultdict(),
        all_extracted_green_skills_dict: dict,
        job_benefits_dict: dict,
    ) -> dict:
        """
        Get skills measures using job advert ids.
        Job advert id's are sometimes strings and sometimes ints depending on how they are loaded/calculated
        so convert them all to strings

        Args:
                        ents_per_job (dict): The job advert ids (keys) and the skill entities predicted
                        all_extracted_green_skills_dict (dict): A dictionary of skills to which green skills they map to
                        job_benefits_dict (dict): The job adverts ids (keys) and the job benefit entities predicted
        Returns:
                        dict: A dictionary of job advert ids and green measures information
        """
        if isinstance(next(iter(all_extracted_green_skills_dict)), int):
            all_extracted_green_skills_dict = {
                str(k): v for k, v in all_extracted_green_skills_dict.items()
            }

        prop_green_skills = {}
        for job_id, split_skill_ents in ents_per_job.items():
            if split_skill_ents:
                num_orig_ents = len(split_skill_ents)
                split_ents = [r for v in split_skill_ents for r in v[0]]
                num_split_ents = len(split_ents)

                green_ents = []
                for skill in split_ents:
                    if all_extracted_green_skills_dict[skill][0] == "green":
                        green_ents.append(
                            (skill, all_extracted_green_skills_dict.get(skill))
                        )

                prop_green_skills[job_id] = {
                    "NUM_ORIG_ENTS": num_orig_ents,
                    "NUM_SPLIT_ENTS": num_split_ents,
                    "ENTS": split_skill_ents,
                    "GREEN_ENTS": green_ents,
                    "PROP_GREEN": len(green_ents) / num_split_ents
                    if num_split_ents != 0
                    else 0,
                    "BENEFITS": job_benefits_dict.get(job_id),
                }
            else:
                prop_green_skills[job_id] = {
                    "NUM_ORIG_ENTS": 0,
                    "NUM_SPLIT_ENTS": 0,
                    "ENTS": None,
                    "GREEN_ENTS": None,
                    "PROP_GREEN": 0,
                    "BENEFITS": job_benefits_dict.get(job_id),
                }

        return prop_green_skills
