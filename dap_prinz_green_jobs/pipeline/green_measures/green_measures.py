"""
A Green Measures class that takes as input a job advert and outputs
    measures of greenness at the skill-, occupation- and industry-level.
"""

from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupations_measures_utils import (
    OccupationMeasures,
)
from dap_prinz_green_jobs.pipeline.green_measures.industries.industries_measures import (
    IndustryMeasures,
)
from dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils import (
    SkillMeasures,
)
from dap_prinz_green_jobs import PROJECT_DIR

from typing import List, Dict, Optional
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
    skills_output_folder (str): If given, this will be the folder where all skills outputs are stored,
        if not given it will default to f"outputs/data/green_skill_lists/{date_stamp}"
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

    def __init__(self, config_name: str = "base", skills_output_folder: str = ""):
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
        self.green_skills_classifier_model_file_name = self.config["skills"][
            "green_skills_classifier_model_file_name"
        ]

        if not skills_output_folder:
            date_stamp = str(date.today().date()).replace("-", "")
            skills_output_folder = f"outputs/data/green_skill_lists/{date_stamp}"

        if self.load_skills:
            self.skills_output = self.config["skills"]["skills_output"]
        else:
            self.skills_output = os.path.join(
                skills_output_folder, "skills_data_ojo_mixed.json"
            )

        if self.load_skills_embeddings:
            self.skill_embeddings_output = self.config["skills"][
                "skill_embeddings_output"
            ]
        else:
            self.skill_embeddings_output = os.path.join(
                skills_output_folder, "extracted_skills_embeddings.json"
            )

        if self.load_taxonomy_embeddings:
            self.green_tax_embedding_path = self.config["skills"][
                "green_tax_embedding_path"
            ]
        else:
            self.green_tax_embedding_path = os.path.join(
                skills_output_folder, "green_esco_embeddings.json"
            )

        # Where to output the mappings of skills to all of ESCO (not just green)
        self.skill_mappings_output_path = os.path.join(
            skills_output_folder, "full_esco_skill_mappings.json"
        )

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
        self.im.load()

        # Skills attributes
        self.sm = SkillMeasures(
            config_name="extract_green_skills_esco",
            green_skills_classifier_model_file_name=self.green_skills_classifier_model_file_name,
        )
        self.sm.initiate_extract_skills(local=False, verbose=True)

    def get_skill_measures(
        self,
        job_advert: Optional[Dict[str, str]] = None,
    ) -> List[dict]:
        if type(job_advert) == dict:
            job_advert = [job_advert]

        taxonomy_skills_embeddings_dict = self.sm.get_green_taxonomy_embeddings(
            output_path=self.green_tax_embedding_path,
            load=self.load_taxonomy_embeddings,
        )

        prop_green_skills = self.sm.get_measures(
            job_advert,
            skills_output_path=self.skills_output,
            load_skills=self.load_skills,
            job_text_key=self.job_text_key,
            job_id_key=self.job_id_key,
            skill_embeddings_output_path=self.skill_embeddings_output,
            load_skills_embeddings=self.load_skills_embeddings,
            skill_mappings_output_path=self.skill_mappings_output_path,
        )

        return prop_green_skills

    def get_occupation_measures(self, job_advert: Dict[str, str]) -> List[dict]:
        """
        Extract measures of greenness at the occupation-level. Measures include:
            - GREEN CATEGORY: O*NET green occupation categorisation
            - GREEN/NOT GREEN: whether occupation name is considered green or not green

        Also returned will be the dictionary of SOC 2020 codes to their name
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
        green_occupation_measures_dict = dict(
            zip([j[self.job_id_key] for j in job_advert], occ_green_measures_list)
        )

        soc_name_dict = {
            "soc_2020_6": self.om.soc_mapper.soc_2020_6_dict,
            "soc_2020_4": self.om.soc_mapper.soc_2020_4_dict,
        }

        return green_occupation_measures_dict, soc_name_dict

    def get_industry_measures(self, job_advert: Dict[str, str]) -> List[dict]:
        """
        Extract measures of greenness at the industry-level. Measures include:
            - INDUSTRY: SIC GHG emissions based on job advert company name
        """
        if isinstance(job_advert, dict):
            job_advert = [job_advert]

        ind_green_measures_dict = self.im.get_measures(job_advert)

        return ind_green_measures_dict

    def get_green_measures(
        self,
        job_advert: Dict[str, str],
    ) -> Dict[str, List[dict]]:
        """
        Extract measures of greenness at the skill-, occupation- and industry-level. Measures include:
            - skills: green skill %, green skill count and the extracted green skills
            - occupations: O*NET green occupation categorisation and whether occupation name is considered green or not green
            - industry: random choice green or not green
        """
        green_measures_dict = {}

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
