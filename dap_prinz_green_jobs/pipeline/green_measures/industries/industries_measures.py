"""
Industries Measures class to extract industry measures for a given job advert or list of job adverts.

Usage:

    from dap_prinz_green_jobs.pipeline.green_measures.industries.industries_measures import IndustryMeasures

    job_ads = {'id': 1, 'company_name': "Fake Company", 'job_text': 'We are looking for a software engineer to join our team. We are a fast growing company in the software engineering industry.'}

    im = IndustryMeasures()
    im.load() #load necessary SIC mapper class and Industry-level greenness datasets
    im.get_measures(job_ads)

    >>  [{'SIC': '582',
    'SIC_name': 'Software publishing',
    'INDUSTRY TOTAL GHG EMISSIONS': 46.4,
    'INDUSTRY GHG PER UNIT EMISSIONS': 0.01,
    'INDUSTRY PROP HOURS GREEN TASKS': 9.700000000000001,
    'INDUSTRY PROP WORKERS GREEN TASKS': 43.5,
    'INDUSTRY PROP WORKERS 20PERC GREEN TASKS': 23.599999999999998}]]


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
    config_name: str
        Name of the config file to use. Default is "base.yaml".
    ----------
    Methods
    ----------
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

        self.closest_distance_threshold = self.config["industries"][
            "closest_distance_threshold"
        ]
        self.majority_sic_threshold = self.config["industries"][
            "majority_sic_threshold"
        ]

    def load(self):
        """
        Method to load necessary SIC mapper class and
            Industry-level greenness datasets.
        """
        self.sm = SicMapper()
        self.sm.load()
        # Dictionary of SIC codes and total GHG emissions and GHG emissions per unit of economy activity
        self.ghg_emissions_dict, self.ghg_unit_emissions_dict = get_clean_ghg_data()
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

        industry_measures_list = []
        for sic_info in sic_codes:
            confidence_threshold = None
            if sic_info["sic_method"] == "closest distance":
                confidence_threshold = self.closest_distance_threshold
            elif sic_info["sic_method"] == "majority":
                confidence_threshold = self.majority_sic_threshold

            if (
                confidence_threshold is not None
                and sic_info["sic_confidence"] > confidence_threshold
            ):
                sic = sic_info["sic_code"]
            elif sic_info["sic_method"] == "companies house":
                sic = sic_info["sic_code"]
            else:
                sic = None

            sic_clean = clean_sic(sic)
            sic_section = self.sic_to_section.get(sic_clean)
            industry_measures = {
                "SIC": sic,
                "SIC_name": sic_info["sic_name"],
                "SIC_confidence": sic_info["sic_confidence"],
                "SIC_method": sic_info["sic_method"],
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
            }
            industry_measures_list.append(industry_measures)

        return industry_measures_list
