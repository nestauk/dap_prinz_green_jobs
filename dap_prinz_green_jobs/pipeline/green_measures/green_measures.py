"""
A Green Measures class that takes as input a job advert and outputs
    measures of greenness at the skill-, occupation- and industry-level.
"""

from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupations_measures_utils import (
    OccupationMeasures,
)
from dap_prinz_green_jobs.pipeline.green_measures.industries.industries_measures_utils import (
    IndustryMeasures,
)
from dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils import (
    SkillMeasures,
    get_green_skill_measures,
)
from dap_prinz_green_jobs import logger, PROJECT_DIR

from typing import List, Union, Dict, Optional
from uuid import uuid4
import yaml
import os
from datetime import datetime as date


class GreenMeasures(object):
    """
    Class to extract green measures at the skill-, occupation and
        industry-level for a given job advert or list of job adverts.
    Attributes
    ----------
    config_name (str): the name of the config file to use.
    ----------
    Methods
    ----------
    get_skill_measures(job_advert=None, skill_list=None):
        for a given job advert or list of job adverts, extract skill-level green measures. If you have
            already extracted raw skills, you can pass a list of skills to avoid re-extracting.
    get_occupation_measures(job_advert):
        for a given job advert or list of job adverts, extract occupation-level green measures.
    get_industry_measures(job_advert):
        for a given job advert or list of job adverts, extract industry-level green measures.
    get_green_measures(job_advert, skill_list=None):
        for a given job advert or list of job adverts, extract skill-, occupation- and industry-level green measures.
            you can also pass a skill list to avoid re-extracting skills.
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

        # Skills config variables
        self.skills_config_name = self.config["skills"]["skills_config_name"]
        self.load_skills = self.config["skills"][
            "load_skills"
        ]  # Set to false if your job adverts or NER model changes
        self.load_skills_embeddings = self.config["skills"][
            "load_skills_embeddings"
        ]  # Set to false if your job advert data, NER model or way to embed changes
        self.load_taxonomy_embeddings = self.config["skills"][
            "load_taxonomy_embeddings"
        ]  # Set to false if your input taxonomy data or way to embed changes

        date_stamp = str(date.today().date()).replace("-", "")

        if self.load_skills:
            self.skills_output = self.config["skills"]["skills_output"]
        else:
            self.skills_output = f"outputs/data/green_skill_lists/skills_data_ojo_mixed_{date_stamp}.json"

        if self.load_skills_embeddings:
            self.skill_embeddings_output = self.config["skills"][
                "skill_embeddings_output"
            ]
        else:
            self.skill_embeddings_output = f"outputs/data/green_skill_lists/extracted_skills_embeddings_{date_stamp}.json"

        if self.load_taxonomy_embeddings:
            self.green_tax_embedding_path = self.config["skills"][
                "green_tax_embedding_path"
            ]
        else:
            self.green_tax_embedding_path = f"outputs/data/green_skill_lists/green_esco_embeddings_{date_stamp}.json"

        # Input job advert data config variables
        self.job_id_key = self.config["job_adverts"]["job_id_key"]
        self.job_text_key = self.config["job_adverts"]["job_text_key"]
        self.job_title_key = self.config["job_adverts"]["job_title_key"]
        self.company_name_key = self.config["job_adverts"]["company_name_key"]

        # Occupation attributes
        self.om = OccupationMeasures()
        self.om.load()

        # Industry attributes
        self.im = IndustryMeasures()
        self.im.load_ch()

        # Skills attributes
        self.sm = SkillMeasures(config_name="extract_green_skills_esco")
        self.sm.initiate_extract_skills(local=False, verbose=True)

    def get_skill_measures(
        self,
        job_advert: Optional[Dict[str, str]] = None,
    ) -> List[dict]:
        if type(job_advert) == dict:
            job_advert = [job_advert]

        predicted_entities = self.sm.get_entities(
            job_advert,
            output_path=self.skills_output,
            load=self.load_skills,
            job_text_key=self.job_text_key,
            job_id_key=self.job_id_key,
        )
        skills_list = []
        for p in predicted_entities.values():
            for ent_type in ["SKILL", "MULTISKILL", "EXPERIENCE"]:
                for skill in p[ent_type]:
                    skills_list.append(skill)

        unique_skills_list = list(set(skills_list))

        all_extracted_skills_embeddings_dict = self.sm.get_skill_embeddings(
            unique_skills_list,
            output_path=self.skill_embeddings_output,
            load=self.load_skills_embeddings,
        )

        taxonomy_skills_embeddings_dict = self.sm.get_green_taxonomy_embeddings(
            output_path=self.green_tax_embedding_path,
            load=self.load_taxonomy_embeddings,
        )

        all_extracted_green_skills_dict = self.sm.map_green_skills()

        prop_green_skills = self.sm.get_measures(
            job_advert_ids=[j[self.job_id_key] for j in job_advert],
            predicted_entities=predicted_entities,
            all_extracted_green_skills_dict=all_extracted_green_skills_dict,
        )
        return prop_green_skills

    def get_occupation_measures(self, job_advert: Dict[str, str]) -> List[dict]:
        """
        Extract measures of greenness at the occupation-level. Measures include:
            - GREEN CATEGORY: O*NET green occupation categorisation
            - GREEN/NOT GREEN: whether occupation name is considered green or not green
        """

        if type(job_advert) == dict:
            job_advert = [job_advert]

        unique_job_titles = list(
            set(
                [
                    job.get(self.job_title_key)
                    for job in job_advert
                    if self.job_title_key in job
                ]
            )
        )

        job_title_2_match = self.om.precalculate_soc_mapper(unique_job_titles)
        occ_green_measures_list = self.om.get_measures(
            job_adverts=job_advert, job_title_key=self.job_title_key
        )

        return occ_green_measures_list

    def get_industry_measures(self, job_advert: Dict[str, str]) -> List[dict]:
        """
        Extract measures of greenness at the industry-level. Measures include:
            - INDUSTRY: SIC GHG emissions based on job advert company name
        """

        if type(job_advert) == dict:
            job_advert = [job_advert]

        ind_green_measures_list = self.im.get_measures(
            job_advert=job_advert, company_name_key=self.company_name_key
        )

        return ind_green_measures_list

    def get_green_measures(
        self, job_advert: Dict[str, str], skill_list: Optional[List[str]] = None
    ) -> Dict[str, List[dict]]:
        """
        Extract measures of greenness at the skill-, occupation- and industry-level. Measures include:
            - skills: green skill %, green skill count and the extracted green skills
            - occupations: O*NET green occupation categorisation and whether occupation name is considered green or not green
            - industry: random choice green or not green

        Can also take as input the output of es.get_skills() (skill_list) to avoid re-extracting skills for
            the .get_skill_measures method.
        """
        green_measures_dict = {}
        if skill_list:
            green_measures_dict["SKILL MEASURES"] = self.get_skill_measures(
                skill_list=skill_list
            )
        else:
            green_measures_dict["SKILL MEASURES"] = self.get_skill_measures(
                job_advert=job_advert
            )

        green_measures_dict["INDUSTRY MEASURES"] = self.get_industry_measures(
            job_advert=job_advert
        )
        green_measures_dict["OCCUPATION MEASURES"] = self.get_occupation_measures(
            job_advert=job_advert
        )

        return green_measures_dict
