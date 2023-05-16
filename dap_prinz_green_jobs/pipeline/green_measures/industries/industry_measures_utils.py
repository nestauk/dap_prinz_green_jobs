"""
Util functions for industry green measures
"""

import pandas as pd
import re
import openpyxl
import random

from dap_prinz_green_jobs.getters.industry_getters import load_companies_house

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


def get_green_industry_measure(company_name):
    return random.choice(["Green", "Not green"])


def clean_sic(sic_name):
    if sic_name:
        sic = str(sic_name.split(" - ")[0])
        if len(sic) == 4:
            return "0" + sic
        else:
            return sic
    else:
        return None


def get_ghg_sic(sic, ghg_emissions_dict):
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


def get_ch_sic(companies_house_cleaned_in_ojo_dict: dict, cleaned_name: str) -> str:
    """
    Pick one SIC for each cleaned name using the Companies House data.
    If there are multiple SICs given for a name then pick one randomly.

    TO DO: Pick based off semantic similarity?

    :param companies_house_cleaned_in_ojo_dict: The companies house data with cleaned
            company name as the key and the various SIC codes given for this cleaned name
            (as there can be multiple)
    :type companies_house_cleaned_in_ojo_dict: dict

    :param cleaned_name: The cleaned company name
    :type cleaned_name: str

    :return: A SIC or None
    :rtype: str or None

    """

    companies_house_data = companies_house_cleaned_in_ojo_dict.get(cleaned_name)
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


def process_companies_house(ojo_company_names_cleaned):
    """
    This can take a while
    """

    companies_house = load_companies_house()
    companies_house["cleaned_name"] = companies_house["CompanyName"].map(
        clean_company_name
    )

    companies_house_in_ojo = companies_house[
        companies_house["cleaned_name"].isin(ojo_company_names_cleaned)
    ]

    # For each cleaned name collate all the SIC's given (dont bother saving null columns)
    companies_house_cleaned_in_ojo_dict = {}
    for cleaned_name, grouped_ch in companies_house_in_ojo.groupby("cleaned_name"):
        grouped_ch.dropna(axis=1, inplace=True)
        companies_house_cleaned_in_ojo_dict[cleaned_name] = grouped_ch[
            grouped_ch.columns.difference(["cleaned_name"])
        ].to_dict(orient="records")

    return companies_house_cleaned_in_ojo_dict


# def map_company_name(
#     company_name: Union[str, List[str]],
#     company_sics: pd.DataFrame()
# ) -> Union[str, List[str]]:
#     """Finds the SIC(s) for a particular company name(s)
#     :param company_name: A company name or list of company names
#     :type company_name: str or a list of string
#     :param company_sics: SICs for each company name
#     :type company_sics: pd.DataFrame()
#     :return: A SOC or list of SOCs for the inputted job titles
#     :rtype: str or list
#     """

#     if job_title.isinstance(str):
#         return soc_job_titles.get(job_title)
#     else:
#         return [soc_job_titles.get(title) for title in job_title]
