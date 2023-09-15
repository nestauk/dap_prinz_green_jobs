"""
Industries Measures class to extract industry measures for a given job advert or list of job adverts.
"""
import random
from typing import List, Union, Dict

from dap_prinz_green_jobs.getters.industry_getters import (
    load_companies_house_dict,
    load_sic,
    load_green_tasks_prop_hours,
    load_green_tasks_prop_workers,
    load_green_tasks_prop_workers_20,
)

from dap_prinz_green_jobs.pipeline.green_measures.industries.industries_measures_utils import (
    create_section_dict,
    get_clean_ghg_data,
    clean_sic,
)


class IndustryMeasures(object):
    """
    Class to extract industry measures for a given job advert or list of job adverts.
    ----------
    Methods
    ----------
    get_measures(job_advert):
        for a given job advert (dict) or list of job adverts (list of dicts), extract the industry-level green measures.
        
    ----------
    Usage
    ----------
    job_ad = {}
    
    im = IndustryMeasures()
    
    im.get_measures("Boots")
    """

    def __init__(
        self,
    ):
        # Dictionary of SIC codes and total GHG emissions and GHG emissions per unit of economy activity
        self.ghg_emissions_dict, self.ghg_unit_emissions_dict = get_clean_ghg_data()

        # Dictionary of 5 digit SIC codes to their SIC section code ({'01621': 'A','01629': 'A','05101': 'B',..})
        sic_data = load_sic()
        self.sic_names = dict(
            zip(sic_data["Most disaggregated level"], sic_data["Description"])
        )
        self.sic_to_section = {
            k: v.strip()
            for k, v in dict(
                zip(sic_data["Most disaggregated level"], sic_data["SECTION"])
            ).items()
        }
        # Dictionary of SIC sector (e.g. "A") to proportion of hours worked spent doing green tasks
        self.sic_section_2_prop_hours = create_section_dict(
            load_green_tasks_prop_hours()
        )
        # Dictionary of SIC sector (e.g. "A") to proportion of workers doing green tasks
        self.sic_section_2_prop_workers = create_section_dict(
            load_green_tasks_prop_workers()
        )
        # Dictionary of SIC sector (e.g. "A") to proportion of workers spending at least 20% of their time doing green tasks per SIC
        self.sic_section_2_prop_workers_20 = create_section_dict(
            load_green_tasks_prop_workers_20()
        )

    def get_green_measure_for_company(
        self,
        company_name: str,
    ) -> Union[float, None]:
        """Gets SIC GHG emissions for a given company name.

        Args:
            company_name (str): Company name to get SIC GHG emissions for

        Returns:
            Union[float, None]: Returns SIC GHG emissions for a given company name
                or None if no match
        """
        # clean company name
        company_name_clean = clean_company_name(company_name)

        ch_sic = self.get_ch_sic(
            cleaned_name=company_name_clean,
        )
        # if sic match then clean sic and get ghg emissions
        if ch_sic:
            clean_ch_sic = clean_sic(ch_sic)
            sic_section = self.sic_to_section.get(clean_ch_sic)
            return {
                "SIC": clean_ch_sic,
                "SIC_name": self.sic_names.get(clean_ch_sic),
                "INDUSTRY TOTAL GHG EMISSIONS": get_ghg_sic(
                    clean_ch_sic, self.ghg_emissions_dict
                ),
                "INDUSTRY GHG PER UNIT EMISSIONS": get_ghg_sic(
                    clean_ch_sic, self.ghg_unit_emissions_dict
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

        else:
            return {
                "SIC": None,
                "INDUSTRY TOTAL GHG EMISSIONS": None,
                "INDUSTRY GHG PER UNIT EMISSIONS": None,
                "INDUSTRY PROP HOURS GREEN TASKS": None,
                "INDUSTRY PROP WORKERS GREEN TASKS": None,
                "INDUSTRY PROP WORKERS 20PERC GREEN TASKS": None,
            }

    def get_measures(
        self, job_advert: Dict[str, str], company_name_key: str
    ) -> Dict[str, List[dict]]:
        if type(job_advert) == dict:
            job_advert = [job_advert]

        ind_green_measures_list = []
        for job in job_advert:
            comp_name = job.get(company_name_key)

            ind_green_measures_list.append(
                self.get_green_measure_for_company(company_name=comp_name)
            )
            # Could add more measures here if we want to use company information from the job advert description
        return ind_green_measures_list
