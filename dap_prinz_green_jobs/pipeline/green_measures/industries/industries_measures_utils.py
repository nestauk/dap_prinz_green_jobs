"""
Util functions for industry green measures
"""
import re
import random
from typing import Union, Dict

from dap_prinz_green_jobs.getters.industry_getters import (
    load_companies_house_dict,
    load_industry_ghg_dict,
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

ojo_companies_house_dict = load_companies_house_dict()
ghg_emissions_dict = load_industry_ghg_dict()
sic_data = load_sic()
sic_to_section = {
    k: v.strip()
    for k, v in dict(zip(sic_data["Sub Class"], sic_data["SECTION"])).items()
}


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


def clean_sic(sic_name):
    if sic_name:
        sic = str(sic_name.split(" - ")[0])
        if len(sic) == 4:
            return "0" + sic
        else:
            return sic
    else:
        return None


def get_ghg_sic(sic, ghg_emissions_dict: Dict[str, float] = ghg_emissions_dict):
    """
    Could do more to find it, but I think it might be best to just clean the emissions data
    """
    if sic:
        sic_2 = sic[0:2]  # 19
        sic_3 = sic[0:2] + "." + sic[2]  # 19.1
        sic_4 = sic[0:2] + "." + sic[2:4]  # 19.12
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


def get_ch_sic(
    cleaned_name: str,
    ojo_companies_house_dict: Dict[str, Dict[str, str]] = ojo_companies_house_dict,
) -> str:
    """
    Pick one 5 digit SIC for each cleaned name using the Companies House data.
    If there are multiple SICs given for a name then pick one randomly.

    TO DO: Pick based off semantic similarity?

    :param ojo_companies_house_dict: The companies house data with cleaned
            company name as the key and the various SIC codes given for this cleaned name
            (as there can be multiple)
    :type companies_house_cleaned_in_ojo_dict: dict

    :param cleaned_name: The cleaned company name
    :type cleaned_name: str

    :return: A SIC or None
    :rtype: str or None

    """
    companies_house_data = ojo_companies_house_dict.get(cleaned_name)
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


def get_green_industry_measure(
    company_name: str,
    sic_section_2_prop_hours: Dict[str, float],
    sic_section_2_prop_workers: Dict[str, float],
    sic_section_2_prop_workers_20: Dict[str, float],
    ojo_companies_house_dict: Dict[str, Dict[str, str]] = ojo_companies_house_dict,
    ghg_emissions_dict: Dict[str, float] = ghg_emissions_dict,
    sic_to_section: Dict[str, str] = sic_to_section,
) -> Union[float, None]:
    """Gets SIC GHG emissions for a given company name.

    Args:
        company_name (str): Company name to get SIC GHG emissions for
        sic_section_2_prop_hours (Dict[str, float]): Dictionary of SIC sector (e.g. "A") to proportion of hours worked spent doing green tasks
        sic_section_2_prop_workers (Dict[str, float]): Dictionary of SIC sector (e.g. "A") to proportion of workers doing green tasks
        sic_section_2_prop_workers_20 (Dict[str, float]): Dictionary of SIC sector (e.g. "A") to proportion of workers spending at least 20% of their time doing green tasks per SIC
        ojo_companies_house_dict (Dict[str, Dict[str, str]]): Dictionary of company names
            and companies house SIC data
        ghg_emissions_dict (Dict[str, float]): Dictionary of SIC codes and GHG emissions
        sic_to_section (Dict[str, str]): Dictionary of 5 digit SIC codes to their SIC section code ({'01621': 'A','01629': 'A','05101': 'B',..})

    Returns:
        Union[float, None]: Returns SIC GHG emissions for a given company name
            or None if no match
    """
    # clean company name
    company_name_clean = clean_company_name(company_name)

    ch_sic = get_ch_sic(
        ojo_companies_house_dict=ojo_companies_house_dict,
        cleaned_name=company_name_clean,
    )
    # if sic match then clean sic and get ghg emissions
    if ch_sic:
        clean_ch_sic = clean_sic(ch_sic)
        ghg_emissions_info = get_ghg_sic(clean_ch_sic, ghg_emissions_dict)
        sic_section = sic_to_section.get(clean_ch_sic)
        return {
            "ghg_emissions_info": ghg_emissions_info,
            "green_tasks_prop_hours": sic_section_2_prop_hours.get(sic_section),
            "green_tasks_prop_workers": sic_section_2_prop_workers.get(sic_section),
            "green_tasks_prop_workers_20": sic_section_2_prop_workers_20.get(
                sic_section
            ),
        }
    else:
        return None
