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
    """Downloads the dataset of greenhouse gas emissions by SIC
    :return: A dataframe of SIC and greenhouse gas emissions
    :rtype: pd.DataFrame()
    """

    return pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/industry_data/atmosphericemissionsghg.xlsx",
        sheet_name="GHG total",
        header=None,
    )


def load_industry_ghg_intensity() -> pd.DataFrame():
    """Downloads the dataset of greenhouse gas emissions per unit of economy activity by SIC
    :return: A dataframe of SIC and greenhouse gas emissions
    :rtype: pd.DataFrame()
    """

    return pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/industry_data/atmosphericemissionsghgintensity.xlsx",
        sheet_name="GHG intensity",
        header=None,
    )


def load_sic() -> pd.DataFrame():
    """Downloads the SIC dataset
    :return: A dataframe of SIC codes and names
    :rtype: pd.DataFrame()
    """
    sic_data = pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/industry_data/publisheduksicsummaryofstructureworksheet.xlsx",
        sheet_name="reworked structure",
    )

    return sic_data


def load_green_tasks_prop_hours() -> pd.DataFrame():
    """Downloads a dataset of the proportion of hours worked spent doing green tasks per SIC
    :return: A dataframe of SIC and proportion of hours worked spent doing green tasks
    :rtype: pd.DataFrame()
    """
    green_tasks_prop_hours = pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/industry_data/greentasks.xlsx",
        sheet_name="Table_6",
    )

    return green_tasks_prop_hours


def load_green_tasks_prop_workers() -> pd.DataFrame():
    """Downloads a dataset of the proportion of workers doing green tasks per SIC
    :return: A dataframe of SIC and proportion of workers doing green tasks
    :rtype: pd.DataFrame()
    """
    green_tasks_prop_workers = pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/industry_data/greentasks.xlsx",
        sheet_name="Table_7",
    )

    return green_tasks_prop_workers


def load_green_tasks_prop_workers_20() -> pd.DataFrame():
    """Downloads a dataset of the proportion of workers spending at least 20% of their time doing green tasks per SIC
    :return: A dataframe of SIC and proportion of workers spending at least 20% of their time doing green tasks
    :rtype: pd.DataFrame()
    """
    green_tasks_prop_workers_20 = pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/industry_data/greentasks.xlsx",
        sheet_name="Table_8",
    )

    return green_tasks_prop_workers_20
