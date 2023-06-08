"""
A Green Measures class that takes as input a job advert and outputs
    measures of greenness at the skill-, occupation- and industry-level.
"""
from ojd_daps_skills.pipeline.extract_skills.extract_skills import (
    ExtractSkills,
)  # import the module
import dap_prinz_green_jobs.pipeline.green_measures.occupations.occupations_measures_utils as om
import dap_prinz_green_jobs.pipeline.green_measures.industries.industries_measures_utils as im
import dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils as sm
from dap_prinz_green_jobs.pipeline.green_measures.occupations.soc_map import SOCMapper
from dap_prinz_green_jobs.getters.industry_getters import (
    load_industry_ghg_dict,
    load_companies_house_dict,
)
from dap_prinz_green_jobs import logger, PROJECT_DIR

from typing import List, Union, Dict, Optional
from uuid import uuid4
import yaml
import os


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
        self.skill_threshold = self.config["skills"]["skill_threshold"]
        self.skills_config_name = self.config["skills"]["skills_config_name"]

        # Input job advert data config variables
        self.job_text_key = self.config["job_adverts"]["job_text_key"]
        self.job_title_key = self.config["job_adverts"]["job_title_key"]
        self.company_name_key = self.config["job_adverts"]["company_name_key"]

        self.green_soc_data = om.get_green_soc_measures()
        self.ghg_emissions_dict = load_industry_ghg_dict()
        self.ojo_companies_house_dict = load_companies_house_dict()
        try:
            self.es = ExtractSkills(self.skills_config_name)
        except FileNotFoundError:
            logger.exception(
                "*** Please run dap_prinz_green_jobs/pipeline/green_measures/skills/customise_skills_extractor.py to add custom config and data files to ojd-daps-skills library folder ***"
            )
        self.es.load()
        self.es.taxonomy_skills.rename(columns={"Unnamed: 0": "id"}, inplace=True)
        self.get_occupation_measures_called = False

    def get_skill_measures(
        self,
        job_advert: Optional[Dict[str, str]] = None,
        skill_list: Optional[str] = None,
    ) -> List[dict]:
        """
        Extract measures of greenness at the skill-level. Measures include:
            - GREEN_SKILL_PERCENT: the percentage of skills in the job advert that are green skills.
            - GREEN_SKILL_COUNT: the number of skills in the job advert that are green skills.
            - GREEN_SKILLS: the list of skills in the job advert that match to a green skill.

        Can also take as input the output of es.get_skills() (skill_list) to avoid re-extracting skills.
        """
        if (not job_advert) and (skill_list):
            if isinstance(skill_list[0], str):
                raw_skills = self.es.format_skills(skill_list)
            raw_skills = skill_list
        if (job_advert) and (not skill_list):
            if type(job_advert) == dict:
                job_advert = [job_advert]
            raw_skills = self.es.get_skills(
                [job[self.job_text_key] for job in job_advert]
            )

        job_skills, skill_hashes = self.es.skill_mapper.preprocess_job_skills(
            {
                "predictions": dict(
                    zip(
                        [str(uuid4()).replace("-", "") for _ in range(len(raw_skills))],
                        [skill for skill in raw_skills],
                    )
                )
            }
        )

        extracted_green_skills = sm.get_green_skill_measures(
            es=self.es,
            raw_skills=raw_skills,
            skill_hashes=skill_hashes,
            job_skills=job_skills,
            skill_threshold=self.skill_threshold,
        )

        # create green measures for each job advert and save to dictionary
        self.green_skill_measures = []
        for i, _ in enumerate(raw_skills):
            if "SKILL" in extracted_green_skills[i].keys():
                matched_skills = [
                    i for i in extracted_green_skills[i]["SKILL"] if i[1][0] != ""
                ]
                green_skill_percent = (
                    len(matched_skills) / len(extracted_green_skills[i]["SKILL"])
                ) * 100
            else:
                matched_skills = 0
                green_skill_percent = 0
            self.green_skill_measures.append(
                {
                    "GREEN SKILL PERCENT": round(green_skill_percent, 2),
                    "GREEN SKILL COUNT": len(matched_skills),
                    "GREEN SKILLS": matched_skills,
                }
            )
        return self.green_skill_measures

    def get_occupation_measures(self, job_advert: Dict[str, str]) -> List[dict]:
        """
        Extract measures of greenness at the occupation-level. Measures include:
            - GREEN CATEGORY: O*NET green occupation categorisation
            - GREEN/NOT GREEN: whether occupation name is considered green or not green
        """
        if type(job_advert) == dict:
            job_advert = [job_advert]

        if not self.get_occupation_measures_called:
            self.soc_green_measures_dict = self.green_soc_data.set_index("SOC_2010")[
                ["GLA_Green Category", "GLA_Green/Non-green", "timeshare_2019"]
            ].T.to_dict()
            self.soc_mapper = SOCMapper(
                local=self.config["occupations"]["local"],
                embeddings_output_dir=self.config["occupations"][
                    "embeddings_output_dir"
                ],
                batch_size=self.config["occupations"]["batch_size"],
                match_top_n=self.config["occupations"]["match_top_n"],
                sim_threshold=self.config["occupations"]["sim_threshold"],
                top_n_sim_threshold=self.config["occupations"]["top_n_sim_threshold"],
                minimum_n=self.config["occupations"]["minimum_n"],
                minimum_prop=self.config["occupations"]["minimum_prop"],
            )
            self.soc_mapper.load(save_embeds=self.config["occupations"]["save_embeds"])

            self.get_occupation_measures_called = True

        # It's quicker to use soc_mapper with a bulk unique input
        unique_job_titles = list(
            set(
                [
                    job.get(self.job_title_key)
                    for job in job_advert
                    if self.job_title_key in job
                ]
            )
        )
        soc_matches = self.soc_mapper.get_soc(job_titles=unique_job_titles)
        job_title_2_match = dict(zip(unique_job_titles, soc_matches))

        occ_green_measures_list = []
        for job in job_advert:
            soc_2020 = job_title_2_match[job.get(self.job_title_key)]
            soc_info = {}
            if soc_2020:
                soc_2020 = soc_2020[
                    0
                ]  # first is the code, second is the job title match
                if len(soc_2020) > 4:
                    soc_info["SOC_2020_EXT"] = soc_2020
                    soc_2020 = soc_2020[0:4]
            soc_info["SOC_2020"] = soc_2020
            # TO DO: THIS MAPPER MIGHT NOT BE 1:1
            soc_2010 = self.soc_mapper.soc_2020_2010_mapper.get(soc_2020)
            soc_info["SOC_2010"] = soc_2010
            green_occ_measures = self.soc_green_measures_dict.get(soc_2010)
            if green_occ_measures:
                occ_green_measures_list.append(
                    {
                        "GREEN CATEGORY": green_occ_measures.get("GLA_Green Category"),
                        "GREEN/NOT GREEN": green_occ_measures.get(
                            "GLA_Green/Non-green"
                        ),
                        "GREEN TIMESHARE": green_occ_measures.get("timeshare_2019"),
                        "SOC": soc_info,
                    }
                )
            else:
                occ_green_measures_list.append(
                    {
                        "GREEN CATEGORY": None,
                        "GREEN/NOT GREEN": None,
                        "GREEN TIMESHARE": None,
                        "SOC": None,
                    }
                )

        return occ_green_measures_list

    def get_industry_measures(self, job_advert: Dict[str, str]) -> List[dict]:
        """
        Extract measures of greenness at the industry-level. Measures include:
            - INDUSTRY: SIC GHG emissions based on job advert company name
        """

        if type(job_advert) == dict:
            job_advert = [job_advert]

        comp_names = [job.get(self.company_name_key) for job in job_advert]

        ind_green_measures_dict = {}
        if comp_names:
            ind_green_measures_dict["INDUSTRY GHG EMISSIONS"] = [
                im.get_green_industry_measure(
                    company_name=comp_name,
                    ghg_emissions_dict=self.ghg_emissions_dict,
                    ojo_companies_house_dict=self.ojo_companies_house_dict,
                )
                for comp_name in comp_names
            ]
        else:
            ind_green_measures_dict["INDUSTRY GHG EMISSIONS"] = None

        return ind_green_measures_dict

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
