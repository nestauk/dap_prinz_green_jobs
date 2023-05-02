"""
Util functions for industry green measures
"""

import pandas as pd
import re
from urllib import parse
from collections import Counter
import json
import openpyxl

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


def load_companies_house() -> pd.DataFrame():
    """Downloads the Companies House dataset
    :return: A dataframe of company information including name and SIC
    :rtype: pd.DataFrame()
    """
    companies_house = pd.read_csv(
        "s3://prinz-green-jobs/inputs/data/industry_data/BasicCompanyDataAsOneFile-2023-05-01_key_columns_only.csv"
    )
    return companies_house


def load_industry_ghg() -> pd.DataFrame():
    """Downloads a dataset of greenhouse gas emissions per SIC
    :return: A dataframe of SIC and greenhouse gas emissions
    :rtype: pd.DataFrame()
    """
    emissions_data = pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/industry_data/atmosphericemissionsghg.xlsx",
        sheet_name="GHG total",
        skiprows=3,
    )
    emissions_data.reset_index(inplace=True)
    emissions_data = emissions_data.loc[list(range(0, 21)) + list(range(26, 156))]

    return emissions_data


def load_sic() -> pd.DataFrame():
    """Downloads a dataset of greenhouse gas emissions per SIC
    :return: A dataframe of SIC and greenhouse gas emissions
    :rtype: pd.DataFrame()
    """
    sic_data = pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/industry_data/publisheduksicsummaryofstructureworksheet.xlsx",
        sheet_name="reworked structure",
    )

    return sic_data


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


# ## TO DO IN OJO APPLICATION EVENTUALLY
# if __name__ == '__main__':

#     companies_house = load_companies_house()
#     companies_house["cleaned_name"] = companies_house["CompanyName"].map(clean_company_name)


#     from dap_prinz_green_jobs.getters.ojo import get_ojo_sample, get_ojo_job_title_sample, get_ojo_location_sample, get_ojo_salaries_sample, get_ojo_skills_sample

#     ojo_data = get_ojo_sample()

#     ojo_data_1 = get_ojo_job_title_sample()


#     # # I dont think the sector/parent sector/knowledge domain match well to SIC
#     # sic_data = load_sic()

#     ## whilst I wait for compnay name to be added
#     with open("/Users/elizabethgallagher/Code/liz_sandpit/prinz/data/20220622_sampled_job_ads.json") as f:
#         ojo_data_2 = json.load(f)
#     ojo_data_2 = pd.DataFrame(ojo_data_2).T
#     ojo_data_2["id"] = ojo_data_2.index.astype(int)
#     ojo_data_2.head(2)

#     ojo_data_2["cleaned_name"] = ojo_data_2["company_raw"].map(clean_company_name)


# name="perfect placement"
# companies_house[companies_house['cleaned_name']==name]
