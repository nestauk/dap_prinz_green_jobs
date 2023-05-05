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
    config_name (str): the name of the config file to use for the skills extractor.
    ----------
    Methods
    ----------
    get_skill_measures(job_advert):
        for a given job advert or list of job adverts, extract skill-level green measures.
    get_occupation_measures(job_advert):
        for a given job advert or list of job adverts, extract occupation-level green measures.
    get_industry_measures(job_advert):
        for a given job advert or list of job adverts, extract industry-level green measures.
    get_green_measures(job_advert):
        for a given job advert or list of job adverts, extract skill-, occupation- and industry-level green measures.
    """

    def __init__(
        self,
        skill_threshold: int = 0.4,
        config_name: str = "extract_green_skills_esco",
        job_text_name: str = "job_text",
        job_title_name: str = "job_title",
        company_name: str = "company_name",
    ):
        self.skill_threshold = skill_threshold
        self.config_name = config_name
        self.job_text_name = job_text_name
        self.job_title_name = job_title_name
        self.company_name = company_name
        self.green_soc_data = (om.load_green_soc(),)
        self.jobtitle_soc_data = (om.load_job_title_soc(),)

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
        try:
            es = ExtractSkills(self.config_name)
        except FileNotFoundError:
            logger.exception(
                "*** Please run dap_prinz_green_jobs/pipeline/green_measures/skills/customise_skills_extractor.py to add custom config and data files to ojd-daps-skills library folder ***"
            )
        es.load()

        es.taxonomy_skills.rename(columns={"Unnamed: 0": "id"}, inplace=True)
        # set skill match threshold as 0 to get all skills
        es.taxonomy_info["match_thresholds_dict"][
            "skill_match_thresh"
        ] = self.skill_threshold

        if (not job_advert) and (skill_list):
            if isinstance(skill_list[0], str):
                raw_skills = es.format_skills(skill_list)
            raw_skills = skill_list
        if (job_advert) and (not skill_list):
            if type(job_advert) == dict:
                job_advert = [job_advert]
            raw_skills = es.get_skills([job[self.job_text_name] for job in job_advert])

        job_skills, skill_hashes = es.skill_mapper.preprocess_job_skills(
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
            es=es,
            raw_skills=raw_skills,
            skill_hashes=skill_hashes,
            job_skills=job_skills,
            skill_threshold=self.skill_threshold,
        )

        # extract green measures for each job advert and save to dictionary
        self.green_skill_measures = []
        for i, _ in enumerate(raw_skills):
            if "SKILL" in extracted_green_skills[i].keys():
                green_skill_percent = (
                    len(extracted_green_skills[i]["SKILL"])
                    / (len(raw_skills[i]["SKILL"]) + len(raw_skills[i]["MULTISKILL"]))
                ) * 100
                green_skill_count = len(extracted_green_skills[i]["SKILL"])
                green_skills = extracted_green_skills[i]["SKILL"]
            else:
                green_skill_percent = 0
                green_skill_count = 0
                green_skills = [{}]
            self.green_skill_measures.append(
                {
                    "GREEN SKILL PERCENT": green_skill_percent,
                    "GREEN SKILL COUNT": green_skill_count,
                    "GREEN SKILLS": green_skills,
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

        soc_2010_4_mapper = om.create_job_title_soc_mapper(self.jobtitle_soc_data[0])

        soc_green_measures_dict = (
            self.green_soc_data[0]
            .set_index("soc_4_2010")[["Green Category", "Green/Non-green"]]
            .T.to_dict()
        )

        occ_green_measures_list = []
        for job in job_advert:
            job_titles = job.get(self.job_title_name)
            job_titles_clean = om.clean_job_title(job_titles)
            job_title_to_soc = soc_2010_4_mapper.get(job_titles_clean)
            green_occ_measures = soc_green_measures_dict.get(job_title_to_soc)
            if green_occ_measures:
                occ_green_measures_list.append(
                    {
                        "GREEN CATEGORY": green_occ_measures.get("Green Category"),
                        "GREEN/NOT GREEN": green_occ_measures.get("Green/Non-green"),
                    }
                )
            else:
                occ_green_measures_list.append(
                    {"GREEN CATEGORY": None, "GREEN/NOT GREEN": None}
                )

        return occ_green_measures_list

    def get_industry_measures(self, job_advert: Dict[str, str]) -> List[dict]:
        """
        Extract measures of greenness at the industry-level. Measures include:
            - INDUSTRY: random choice green/not green industry classification
        """

        if type(job_advert) == dict:
            job_advert = [job_advert]

        ind_green_measures_list = []
        for job in job_advert:
            comp_names = job.get(self.company_name)
            if comp_names:
                comp_name_clean = im.clean_company_name(comp_names)
                ind_green_measures = im.get_green_industry_measure(comp_name_clean)
                ind_green_measures_list.append({"INDUSTRY": ind_green_measures})
            else:
                ind_green_measures_list.append({"INDUSTRY": None})

        return ind_green_measures_list

    def get_green_measures(
        self, job_advert: Dict[str, str], skill_list: Optional[List[str]] = None
    ) -> List[dict]:
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
