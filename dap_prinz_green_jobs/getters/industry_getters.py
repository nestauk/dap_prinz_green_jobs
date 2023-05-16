import pandas as pd
import openpyxl
from typing import Dict

from dap_prinz_green_jobs import BUCKET_NAME
from dap_prinz_green_jobs.getters.data_getters import load_s3_data


def load_companies_house() -> pd.DataFrame():
    """Downloads the Companies House dataset
    :return: A dataframe of company information including name and SIC
    :rtype: pd.DataFrame()
    """
    companies_house = pd.read_csv(
        "s3://prinz-green-jobs/inputs/data/industry_data/BasicCompanyDataAsOneFile-2023-05-01_key_columns_only.csv"
    )
    return companies_house


def load_companies_house_dict() -> Dict[str, Dict[str, dict]]:
    """Loads companies house dictionary"""
    return load_s3_data(
        BUCKET_NAME, "outputs/data/green_industries/companies_house_dict.json"
    )


def load_industry_ghg() -> pd.DataFrame():
    """Downloads a dataset of greenhouse gas emissions per SIC
    :return: A dataframe of SIC and greenhouse gas emissions
    :rtype: pd.DataFrame()
    """

    return load_s3_data(
        BUCKET_NAME, "outputs/data/green_industries/ghg_emissions_data.csv"
    )


def load_industry_ghg_dict() -> Dict[str, float]:
    """Loads industry GHG emissions dictionary"""
    return load_s3_data(BUCKET_NAME, "outputs/data/green_industries/ghg_dict.json")


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
