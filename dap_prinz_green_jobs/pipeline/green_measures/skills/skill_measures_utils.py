from dap_prinz_green_jobs.utils.bert_vectorizer import BertVectorizer
from dap_prinz_green_jobs import PROJECT_DIR, get_yaml_config, BUCKET_NAME, logger
from dap_prinz_green_jobs.getters.data_getters import save_to_s3, load_s3_data
from dap_prinz_green_jobs.utils.processing import list_chunks

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


def format_skills(skill_label: List[str]) -> List[Dict[str, list]]:
    """Format extracted skills into a dictionary with the following keys:
    EXPERIENCE, SKILL, MULTISKILL

    Args:
            skill_label (List[str]): List of extracted skills

    Returns:
            List[Dict[str, list]]: Formatted list of a dictionary of extracted skills
    """
    if type(skill_label) == list:
        return [{"SKILL": [], "MULTISKILL": skill_label, "EXPERIENCE": []}]
    else:
        return skill_label


def get_green_skill_matches(
    extracted_skill_list: List[str],
    similarities: np.array,
    green_skills_taxonomy: pd.DataFrame(),
    skill_threshold: float = 0.7,
) -> List[Tuple[str, Tuple[str, int]]]:
    """Get green skill matches for a list of extracted skills - use this
            in extract_green_measures flow instead of get_green_skill_measures

            NOTE: this is because speeds up skills mapping considerably
            and because the esco green taxonomy is not hierarchical so we are simply
            matching the extracted skills to the green taxonomy based on a minimum
            threshold cosine similarity.

    Args:
            extracted_skill_list (List[str]): List of extracted skills

    Returns:
            List[Tuple[str, Tuple[str, int]]]: List of tuples with the extracted
                            skill; the mapped green skill and a green skill id
    """
    skill_top_green_skills = []
    for skill_ix, skill in tqdm(enumerate(extracted_skill_list)):
        top_skill_match = np.flip(np.sort(similarities[skill_ix]))[0:1]
        if top_skill_match[0] > skill_threshold:
            green_skill_ix = np.flip(np.argsort(similarities[skill_ix]))[0:1]
            green_skill = green_skills_taxonomy.iloc[green_skill_ix].description.values[
                0
            ]
            skill_top_green_skills.append((skill, (green_skill, skill_ix)))
        else:
            skill_top_green_skills.append((skill, None))

    return skill_top_green_skills


def get_green_skill_measures(
    es: ExtractSkills,
    raw_skills,
    skill_hashes: Dict[int, str],
    job_skills: Dict[str, Dict[str, int]],
    skill_threshold: int = 0.5,
) -> List[dict]:
    """Extract green skills for job adverts.

    Args:
            es (ExtractSkills): instantiated ExtractSkills class
            skill_hashes (Dict[int, str]): Dictionary of skill hashes and skill names
            job_skills (Dict[str, Dict[str, int]]): dictionary of ids and extracted raw skills
            skill_threshold (int, optional): skill semantic similarity. Defaults to 0.5.

    Returns:
            List[dict]: list of dictionaries of green skills
    """

    # to get the output with the top ten closest skills
    mapped_skills = es.skill_mapper.map_skills(
        es.taxonomy_skills,
        skill_hashes,
        es.taxonomy_info.get("num_hier_levels"),
        es.taxonomy_info.get("skill_type_dict"),
    )

    matched_skills = []
    for i, (_, skill_info) in enumerate(job_skills.items()):
        job_skill_hashes = skill_info["skill_hashes"]
        job_skill_info = [
            sk for sk in mapped_skills if sk["ojo_skill_id"] in job_skill_hashes
        ]
        matched_skills_formatted = []
        for job_skill in job_skill_info:
            if job_skill["top_tax_skills"][0][2] > skill_threshold:
                matched_skills_formatted.append(
                    (
                        job_skill["ojo_ner_skill"],
                        (
                            job_skill["top_tax_skills"][0][0],
                            job_skill["top_tax_skills"][0][1],
                        ),
                    )
                )
            else:
                matched_skills_formatted.append(
                    (
                        job_skill["ojo_ner_skill"],
                        ("", 0),
                    )
                )
        matched_skills.append(
            {
                "SKILL": matched_skills_formatted,
                "EXPERIENCE": raw_skills[i]["EXPERIENCE"],
            }
        )

    return matched_skills


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
            # instantiate bert model
            bert_model = BertVectorizer(verbose=True, multi_process=True).fit()

            all_extracted_skills_embeddings = []
            for batch_texts in tqdm(list_chunks(skills_list, 1000)):
                all_extracted_skills_embeddings.append(
                    bert_model.transform(batch_texts)
                )
            all_extracted_skills_embeddings = np.concatenate(
                all_extracted_skills_embeddings
            )

            # create dict
            self.all_extracted_skills_embeddings_dict = dict(
                zip(skills_list, all_extracted_skills_embeddings)
            )

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

            # instantiate bert model
            bert_model = BertVectorizer(verbose=True, multi_process=True).fit()

            # embed clean skills
            taxonomy_skills_embeddings = bert_model.transform(
                self.formatted_taxonomy["description"].to_list()
            )

            # create dict
            self.taxonomy_skills_embeddings_dict = dict(
                zip(
                    list(self.formatted_taxonomy.index),
                    np.array(taxonomy_skills_embeddings),
                )
            )

            if output_path:
                logger.info(f"Saving taxonomy embeddings to {output_path}")
                save_to_s3(
                    BUCKET_NAME, self.taxonomy_skills_embeddings_dict, output_path
                )

        return self.taxonomy_skills_embeddings_dict

    def map_green_skills(self) -> dict:
        """
        Map skills to the green taxonomy of skills

        Returns:
                        dict: A dictionary of skills to the green taxonomy skill they map to (if any)
        """

        logger.info("Mapping green skills...")
        similarities = cosine_similarity(
            np.array(list(self.all_extracted_skills_embeddings_dict.values())),
            np.array(list(self.taxonomy_skills_embeddings_dict.values())),
        )
        # Top matches for skill chunk
        top_green_skills = get_green_skill_matches(
            extracted_skill_list=list(self.all_extracted_skills_embeddings_dict.keys()),
            similarities=similarities,
            green_skills_taxonomy=self.formatted_taxonomy,
            skill_threshold=self.skill_threshold,
        )

        all_extracted_green_skills_dict = {
            sk[0]: sk[1] for sk in top_green_skills if sk[1]
        }

        return all_extracted_green_skills_dict

    def get_measures(
        self,
        job_advert_ids: list,
        predicted_entities: dict,
        all_extracted_green_skills_dict: dict,
    ) -> dict:
        """
        Get skills measures using job advert ids.
        Job advert id's are sometimes strings and sometimes ints depending on how they are loaded/calculated
        so convert them all to strings

        Args:
                        job_advert_ids (list): A list of job advert ids
                        predicted_entities (dict): A dictionary of job advert id to predicted entities
                        all_extracted_green_skills_dict (dict): A dictionary of skills to which green skills they map to
        Returns:
                        dict: A dictionary of job advert ids and green measures information
        """
        if isinstance(job_advert_ids[0], int):
            job_advert_ids = [str(j) for j in job_advert_ids]
        if isinstance(next(iter(predicted_entities)), int):
            predicted_entities = {str(k): v for k, v in predicted_entities.items()}
        if isinstance(next(iter(all_extracted_green_skills_dict)), int):
            all_extracted_green_skills_dict = {
                str(k): v for k, v in all_extracted_green_skills_dict.items()
            }

        prop_green_skills = {}
        for job_id in job_advert_ids:
            pred_skills = predicted_entities.get(job_id)
            if pred_skills:
                ent_types = []
                ents = []
                green_ents = []
                for ent_name in ["SKILL", "MULTISKILL", "EXPERIENCE"]:
                    for ent in pred_skills[ent_name]:
                        ent_types.append(ent_name)
                        ents.append(ent)
                        if all_extracted_green_skills_dict.get(ent):
                            green_ents.append(
                                (ent, all_extracted_green_skills_dict.get(ent))
                            )
                prop_green_skills[job_id] = {
                    "NUM_ENTS": len(ents),
                    "ENTS": ents,
                    "ENT_TYPES": ent_types,
                    "GREEN_ENTS": green_ents,
                    "PROP_GREEN": len(green_ents) / len(ents) if len(ents) != 0 else 0,
                }
            else:
                prop_green_skills[job_id] = {
                    "NUM_ENTS": None,
                    "ENTS": None,
                    "ENT_TYPE": None,
                    "GREEN_ENTS": None,
                    "PROP_GREEN": None,
                }

        return prop_green_skills
