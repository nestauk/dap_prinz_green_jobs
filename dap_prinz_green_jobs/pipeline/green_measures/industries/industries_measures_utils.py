"""
Util functions for industry green measures
"""
import re
import random
from typing import List, Union, Dict, Optional

from dap_prinz_green_jobs.getters.industry_getters import (
    load_companies_house_dict,
    load_industry_ghg,
    load_industry_ghg_intensity,
    load_sic,
    load_green_tasks_prop_hours,
    load_green_tasks_prop_workers,
    load_green_tasks_prop_workers_20,
)

# Found using the most common words in CH data
company_stop_words = set(
    [
        "limited",
        "inc",
        "llc",
        "ltd",
        "apps",
        "co",
        "the",
        "services",
        "management",
        "company",
        "uk",
        "c",
        "llp",
        "lp",
        "international",
        "group",
        "cic",
        "plc",
    ]
)


def create_section_dict(data):
    """
    For the green task proportions per SIC section the data needs a little bit of cleaning.
    Will output in the form {'A': 5.4,'B': 17,'C': 12.1,...}
    """
    data = data.copy().T
    data.columns = data.iloc[0]
    data = data.iloc[1:]
    return dict(zip(data["SIC 2007 section code"], data[2019]))


def clean_company_name(
    name: str,
    word_mapper: dict = {},
    company_stop_words: set = company_stop_words,
) -> str:
    """Clean the company name so it can be matched across datasets

    There is lots of different ways to write company names, so this normalises
    across datasets for matching. e.g. "Apple ltd." and "apple limited"

    :param name: A company name
    :type: str
    :param word_mapper: A dict of words to replace with others (e.g. {"ltd": "limited"})
    :type: dict
    :param company_stop_words: words to be removed from the name
    :type: set
    :return: A cleaned company name
    :rtype: str
    """

    if name:
        name = str(name)
        name = re.sub(r"[^\w\s]", "", name)
        name = " ".join(name.split())  # sort out double spaces and trailing spaces
        name = name.lower()

        name_words = name.split()
        words = [
            word_mapper.get(word, word)
            for word in name_words
            if word not in company_stop_words
        ]

        name = " ".join(words)

        return name
    else:
        return None


def clean_sic(sic_name):
    if sic_name:
        sic = str(sic_name.split(" - ")[0])
        if len(sic) == 4:
            return "0" + sic
        else:
            return sic
    else:
        return None


def get_ghg_sic(sic, ghg_emissions_dict: Dict[str, float]):
    """
    Could do more to find it, but I think it might be best to just clean the emissions data
    """
    if sic:
        sic_2 = sic[0:2]  # 19
        sic_3 = sic[0:3]  # 191
        sic_4 = sic[0:4]  # 1912
        if sic_2 in ghg_emissions_dict:
            return ghg_emissions_dict[sic_2]
        elif sic_3 in ghg_emissions_dict:
            return ghg_emissions_dict[sic_3]
        elif sic_4 in ghg_emissions_dict:
            return ghg_emissions_dict[sic_4]
        else:
            return None
    else:
        return None


def clean_total_emissions_dict(ghg_emissions_dict):
    ghg_emissions_dict_cleaned = {}
    for ghg_sic, ghg in ghg_emissions_dict.items():
        if isinstance(ghg_sic, str):
            ghg_emissions_dict_cleaned[ghg_sic] = ghg
        elif ghg_sic < 10:
            # 9 -> 09
            ghg_emissions_dict_cleaned["0" + str(ghg_sic)] = ghg
        else:
            # if there is a decimal, then remove it 14.3 -> 143
            ghg_emissions_dict_cleaned["".join(str(ghg_sic).split("."))] = ghg

    return ghg_emissions_dict_cleaned


def clean_unit_emissions_dict(ghg_unit_emissions_dict):
    # This should cover all the latest dataset's "unsual" SICs
    sic_ghg_per_unit_cleaner = {
        "10.2-3": ["102", "103"],
        "11.01-06": ["1101", "1102", "1103", "1104", "1105", "1106"],
        "11.01-6": ["1101", "1102", "1103", "1104", "1105", "1106"],
        "20.11 + 20.13": ["2011", "2013"],
        "20.11+20.13+20.15": ["2011", "2013", "2015"],
        "20.14+20.16+20.17+20.6": ["2014", "2016", "2017", "206"],
        "20.12+20.2": ["2012", "202"],
        "23.1-4 & 23.7-9": ["231", "232", "233", "234", "237", "238", "239"],
        "23.5-6": ["235", "236"],
        "24.1-3": ["241", "242", "243"],
        "24.4-5 (not 24.42 nor 24.46)": [
            "2410",
            "2420",
            "2431",
            "2432",
            "2433",
            "2434",
            "2441",
            "2443",
            "2444",
            "2445",
            "245",
        ],
        "24.4-5": ["244", "245"],
        "25.1-3+25.5-9": ["251", "252", "253", "255", "256", "257", "258", "259"],
        "30.2+4+9": ["302", "304", "309"],
        "33 (not 33.15-16)": ["3311", "3312", "3313", "3314", "3317", "3319", "3320"],
        "35.2-3": ["352", "353"],
        "49.1-2": ["491", "492"],
        "49.3-5": ["493", "494", "495"],
        "65.1-2": ["651", "652"],
        "68.1-2": ["681", "682"],
        "84 (not 84.22)": ["841", "8421", "8423", "8424", "8425", "843"],
    }

    ghg_unit_emissions_dict_cleaned = {}
    for ghg_sic, ghg in ghg_unit_emissions_dict.items():
        if isinstance(ghg_sic, str):
            if ghg_sic in sic_ghg_per_unit_cleaner:
                for cleaned_sic in sic_ghg_per_unit_cleaner[ghg_sic]:
                    ghg_unit_emissions_dict_cleaned[cleaned_sic] = ghg
            else:
                ghg_unit_emissions_dict_cleaned[ghg_sic] = ghg
        elif ghg_sic < 10:
            # 9 -> 09
            ghg_unit_emissions_dict_cleaned["0" + str(ghg_sic)] = ghg
        else:
            # if there is a decimal, then remove it 14.3 -> 143
            ghg_unit_emissions_dict_cleaned["".join(str(ghg_sic).split("."))] = ghg

    return ghg_unit_emissions_dict_cleaned


def get_clean_ghg_data():
    """
    Load and clean the GHG datasets (total GHG by SIC and GHG per unit of enconomic activity by SIC)
    to create a SIC to GHG dict e.g. {..., '36': 904.8, '37': 2814.4, '38': 21132.8, ...}

    The GHG data has some inconsistency with how the SICs are quoted "65.1", "80", "84 (not 84.22)"
    so we add a manual cleaning step.

    Also sometimes the GHG emissions are given for grouped SICs (e.g. '20.11+20.13+20.15').
    In the total GHG we aren't able to use any rows which have GHG given for merged SICs. e.g. 10.2-3.
    However, for the GHG per unit of economic output, we can assume this value is the same for each SIC given
    The assumption is, e.g. if the SIC is 352 then it will have the GHG per unit of economic activity as given in the '35.2-3' row

    The datasets also have a few blank rows which we remove first.
    """

    # 1. Total GHG emissions by SIC

    emissions_data = load_industry_ghg()
    emissions_data.reset_index(inplace=True)
    emissions_data.iloc[3, 1] = "SIC"
    emissions_data.columns = emissions_data.iloc[3]
    emissions_data = emissions_data.loc[list(range(4, 24)) + list(range(30, 159))]

    ghg_emissions_dict = dict(
        zip(emissions_data["SIC"].tolist(), emissions_data[2020].tolist())
    )

    ghg_emissions_dict_cleaned = clean_total_emissions_dict(ghg_emissions_dict)

    # 2. GHG per unit of economic activity by SIC

    unit_emissions = load_industry_ghg_intensity()
    unit_emissions.reset_index(inplace=True)
    unit_emissions.iloc[3, 1] = "SIC"
    unit_emissions.columns = unit_emissions.iloc[3]
    unit_emissions = unit_emissions.loc[list(range(4, 24)) + list(range(29, 140))]

    ghg_unit_emissions_dict = dict(
        zip(unit_emissions["SIC"].tolist(), unit_emissions[2021].tolist())
    )

    ghg_unit_emissions_dict_cleaned = clean_unit_emissions_dict(ghg_unit_emissions_dict)

    return ghg_emissions_dict_cleaned, ghg_unit_emissions_dict_cleaned


class IndustryMeasures(object):
    """
    Class to extract industry measures for a given job advert or list of job adverts.
    Currently just based off the company name field.

    ----------
    Methods
    ----------
    get_measures(job_advert, company_name_key):
        for a given job advert (dict) or list of job adverts (list of dicts), extract the industry-level green measures.

    ----------
    Usage
    ----------
    im = IndustryMeasures()
    im.load_ch()
    im.get_green_measure_for_company("Boots")

    or
    im.get_measures(job_advert= [{'description': 'We are looking for a sales ...', 'company_name': 'Boots'}], company_name_key='company_name')

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

    def load_ch(self):
        """
        Keep this separate from init since it takes a while to load
        """
        # Dictionary of company names and companies house SIC data
        self.ojo_companies_house_dict = load_companies_house_dict()

    def get_ch_sic(
        self,
        cleaned_name: str,
    ) -> str:
        """
        Pick one 5 digit SIC for each cleaned name using the Companies House data.
        If there are multiple SICs given for a name then pick one randomly.

        TO DO: Pick based off semantic similarity?

        :type companies_house_cleaned_in_ojo_dict: dict

        :param cleaned_name: The cleaned company name
        :type cleaned_name: str

        :return: A SIC or None
        :rtype: str or None

        """
        companies_house_data = self.ojo_companies_house_dict.get(cleaned_name)
        if companies_house_data:
            sic_options = [
                c["SICCode.SicText_1"]
                for c in companies_house_data
                if c["SICCode.SicText_1"]
            ]
            random.seed(42)
            return random.choice(sic_options)
        else:
            return None

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
