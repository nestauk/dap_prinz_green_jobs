"""
Industries Measures class to extract industry measures for a given job advert or list of job adverts.

Usage:

    from dap_prinz_green_jobs.pipeline.green_measures.industries.industries_measures import IndustryMeasures

    job_ads = [{'id': 1, 'job_text': 'We are looking for a software engineer to join our team. This company sits in the software engineering industry.'}]

    im = IndustryMeasures() #instantiate the class
    im.load() #load the relevant green industries datasets and SicMapper class

    im.get_measures(job_ads) #get the measures for the job advert

    >>  {1: {'SIC': '62012',
  'SIC_name': 'Business and domestic software development',
  'SIC_confidence': 0.62,
  'SIC_method': 'closest distance',
  'company_description': 'This company sits in the software engineering industry..',
  'INDUSTRY TOTAL GHG EMISSIONS': 254,
  'INDUSTRY GHG PER UNIT EMISSIONS': 0,
  'INDUSTRY PROP HOURS GREEN TASKS': 9.700000000000001,
  'INDUSTRY PROP WORKERS GREEN TASKS': 43.5,
  'INDUSTRY PROP WORKERS 20PERC GREEN TASKS': 23.599999999999998,
  'INDUSTRY GHG EMISSIONS PER EMPLOYEE': 0.6,
  'INDUSTRY CARBON DIOXIDE EMISSIONS PER EMPLOYEE': 771.2}}
"""
from typing import List, Union, Dict, Union
import yaml
import os
import pandas as pd

from dap_prinz_green_jobs import PROJECT_DIR, logger

# load industry-level green job measures from yaml file
from dap_prinz_green_jobs.getters.industry_getters import (
    load_green_tasks_prop_hours,
    load_green_tasks_prop_workers,
    load_green_tasks_prop_workers_20,
)

from dap_prinz_green_jobs.pipeline.green_measures.industries.industries_measures_utils import (
    get_clean_ghg_data,
    get_clean_employee_emissions_data,
    create_section_dict,
    get_ghg_sic,
)

from dap_prinz_green_jobs.pipeline.green_measures.industries.sic_mapper.sic_mapper import (
    SicMapper,
)
from dap_prinz_green_jobs.pipeline.green_measures.industries.sic_mapper.sic_mapper_utils import (
    clean_sic,
)


class IndustryMeasures(object):
    """
    Class to extract industry measures for a given job advert or list of job adverts.
    ----------
    Parameters
    ----------
    closest_distance_threshold: float
        Threshold for the closest distance between an extracted company description and a SIC code.

    majority_sic_threshold: float
        Threshold for the majority SIC code confidence.
    ----------
    Methods
    ----------
    load():
        Method to load necessary SIC mapper class and Industry-level greenness datasets.
    get_measures(job_advert):
        For a given job advert (dict) or list of job adverts (list of dicts),
            extract the industry-level green measures.
    ----------
    Usage:

    job_ads = {'id': 1, 'company_name': "Company A", 'job_text': 'We are looking for a software engineer to join our team. We are a fast growing company in the software engineering industry.'}

    im = IndustryMeasures()

    im.load()
    im.get_measures(job_ads)
    """

    def __init__(
        self,
        closest_distance_threshold: float = 0.5,
        majority_sic_threshold: float = 0.3,
        use_gpu: bool = False,
        chunk_size: int = 100,
    ):
        self.closest_distance_threshold = (closest_distance_threshold,)
        self.majority_sic_threshold = majority_sic_threshold
        self.use_gpu = use_gpu
        self.chunk_size = chunk_size

    def load(self):
        """
        Method to load necessary SIC mapper class and
            Industry-level greenness datasets.
        """
        self.sm = SicMapper(use_gpu=self.use_gpu, chunk_size=self.chunk_size)
        self.sm.load()

        # can tune the thresholds here
        self.sm.closest_distance_threshold = self.closest_distance_threshold
        self.sm.majority_sic_threshold = self.majority_sic_threshold

        # Dictionary of SIC codes and total GHG emissions and GHG emissions per unit of economy activity
        self.ghg_emissions_dict, self.ghg_unit_emissions_dict = get_clean_ghg_data()
        # Dictionary of SIC codes and GHG emissions and carbon dioxide emissions per employee
        (
            self.ghg_employee_dict,
            self.carbon_employee_dict,
        ) = get_clean_employee_emissions_data()
        # Dictionary of SIC sector (e.g. "A") to proportion of hours worked spent doing green tasks
        self.sic_section_2_prop_hours = create_section_dict(
            load_green_tasks_prop_hours()
        )
        # Dictionary of SIC sector (e.g. "A") to proportion of workers doing green tasks
        self.sic_section_2_prop_workers = create_section_dict(
            load_green_tasks_prop_workers()
        )
        # Dictionary of SIC sector (e.g. "A") to proportion of workers spending at least 20% of
        # their time doing green tasks per SIC
        self.sic_section_2_prop_workers_20 = create_section_dict(
            load_green_tasks_prop_workers_20()
        )

    def get_measures(
        self, job_adverts: Union[Dict[str, str], List[Dict[str, str]]]
    ) -> List[Dict[str, float]]:
        """Extract industry-level green measures for a given job advert
            or list of job adverts.

        Args:
            job_adverts Union[Dict[str, str], List[Dict[str, str]]]: A job advert
                as a dictionary or list of dictionaries.

        Returns:
            Dict[str, float]: Industry-level green measures
                for a given job advert or list of job adverts.
        """

        sic_codes = self.sm.get_sic_codes(job_adverts)

        industry_measures_dict = {}
        for job_id, sic_info in sic_codes.items():
            sic_code = sic_info["sic_code"]
            # clean sic code if not none else return none
            sic_clean = clean_sic(sic_code) if sic_code else None
            sic_section = self.sm.sic_to_section.get(sic_clean)
            industry_measures_dict[job_id] = {
                "SIC": sic_code,
                "SIC_name": sic_info["sic_name"],
                "SIC_confidence": sic_info["sic_confidence"],
                "SIC_method": sic_info["sic_method"],
                "company_description": sic_info["company_description"],
                "INDUSTRY TOTAL GHG EMISSIONS": get_ghg_sic(
                    sic_clean, self.ghg_emissions_dict
                ),
                "INDUSTRY GHG PER UNIT EMISSIONS": get_ghg_sic(
                    sic_clean, self.ghg_unit_emissions_dict
                ),
                "INDUSTRY PROP HOURS GREEN TASKS": self.sic_section_2_prop_hours.get(
                    sic_section
                ),
                "INDUSTRY PROP WORKERS GREEN TASKS": self.sic_section_2_prop_workers.get(
                    sic_section
                ),
                "INDUSTRY PROP WORKERS 20PERC GREEN TASKS": self.sic_section_2_prop_workers_20.get(
                    sic_section
                ),
                "INDUSTRY GHG EMISSIONS PER EMPLOYEE": self.ghg_employee_dict.get(
                    sic_section
                ),
                "INDUSTRY CARBON DIOXIDE EMISSIONS PER EMPLOYEE": self.carbon_employee_dict.get(
                    sic_section
                ),
            }

        return industry_measures_dict
