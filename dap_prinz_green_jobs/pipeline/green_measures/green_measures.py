"""
A Green Measures class that takes as input a job advert and outputs
    measures of greenness at the skill-, occupation- and industry-level.
"""
from ojd_daps_skills.pipeline.extract_skills.extract_skills import (
    ExtractSkills,
)  # import the module
import dap_prinz_green_jobs.pipeline.green_measures.occupations.occupation_measures_utils as om
import dap_prinz_green_jobs.pipeline.green_measures.industries.industry_measures_utils as im
import dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils as sm
from dap_prinz_green_jobs.pipeline.green_measures.occupations.soc_map import SOCMapper

from dap_prinz_green_jobs.getters.industry_getters import (
    load_industry_ghg_dict,
    load_companies_house_dict,
)

from dap_prinz_green_jobs import logger
from typing import List, Union, Dict, Optional
from uuid import uuid4


class GreenMeasures(object):
    """
    Class to extract green measures at the skill-, occupation and
        industry-level for a given job advert or list of job adverts.
    Attributes
    ----------
    skill_threshold (float): the minimum skill_match_threshold to be considered a match to a taxonomy skill.
    skills_config_name (str): the name of the config file to use for the skills extractor.
    job_text_key (str): the name of the job text key in the job advert.
    job_title_key (str): the name of the job title key in the job advert.
    company_name_key (str): the name of the company key in the job advert.
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
        skill_threshold: int = 0.7,
        skills_config_name: str = "extract_green_skills_esco",
        job_text_key: str = "job_text",
        job_title_key: str = "job_title",
        company_name_key: str = "company_name",
    ):
        self.skill_threshold = skill_threshold
        self.skills_config_name = skills_config_name
        self.job_text_key = job_text_key
        self.job_title_key = job_title_key
        self.company_name_key = company_name_key
        self.green_soc_data = om.load_green_soc()
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
            self.soc_green_measures_dict = self.green_soc_data.set_index("soc_4_2010")[
                ["Green Category", "Green/Non-green"]
            ].T.to_dict()
            self.soc_mapper = SOCMapper()
            self.soc_mapper.load()

            self.get_occupation_measures_called = True

        # It's quicker to use soc_mapper with a bulk unique input
        ix_2_job_titles = {
            i: job.get(self.job_title_key) for i, job in enumerate(job_advert)
        }
        unique_job_titles = list(set(ix_2_job_titles.values()))
        soc_matches = self.soc_mapper.get_soc(job_titles=unique_job_titles)
        job_title_2_match = dict(zip(unique_job_titles, soc_matches))

        occ_green_measures_list = []
        for job in job_advert:
            soc = job_title_2_match[job.get(self.job_title_key)]
            if soc:
                soc = soc[0]
            green_occ_measures = self.soc_green_measures_dict.get(soc)
            if green_occ_measures:
                occ_green_measures_list.append(
                    {
                        "GREEN CATEGORY": green_occ_measures.get("Green Category"),
                        "GREEN/NOT GREEN": green_occ_measures.get("Green/Non-green"),
                        "SOC": soc,
                    }
                )
            else:
                occ_green_measures_list.append(
                    {"GREEN CATEGORY": None, "GREEN/NOT GREEN": None, "SOC": None}
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
