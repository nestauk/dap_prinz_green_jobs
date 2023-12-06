import pandas as pd
from dap_prinz_green_jobs import BUCKET_NAME
from dap_prinz_green_jobs.getters.data_getters import load_s3_data

# THIS IS TO EDIT THE PATHS TO THE DATA


def load_job_title_soc() -> pd.DataFrame():
    """
    Load the dataset which gives SOC codes for thousands of job titles
    """

    jobtitle_soc_data = pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/occupation_data/ons/indexsocextv5updated.xlsx",
        sheet_name="SOC 2020 6 Digit Index",
        converters={"SOC 2020 Ext Code": str, "SOC 2020": str, "SOC 2010": str},
    )

    return jobtitle_soc_data


def load_soc_descriptions() -> pd.DataFrame():
    """
    Load the dataset which gives descriptions for the SOC 6-digit codes
    """

    soc_descriptions = pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/occupation_data/ons/extendedsoc2020structureanddescriptionsexcel121023.xlsx",
        sheet_name="Extended SOC descriptions MG1-9",
    )

    return soc_descriptions


def load_green_gla_soc() -> pd.DataFrame():
    """
    Load the greater london authority dataset which gives green measures from SOC codes
    """

    green_gla_data = pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/occupation_data/gla/Summary of green occupations (Nov 2021).xlsx",
        sheet_name="1. List of all occupations",
        skiprows=3,
        converters={"SOC2010 4-digit": str},
    )
    return green_gla_data


def load_green_timeshare_soc() -> pd.DataFrame():
    """
    Load the ONS's green timeshare dataset per SOC
    """
    green_timeshares = pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/occupation_data/ons/greentimesharesoc.xlsx",
        sheet_name="SOC_2010",
        skiprows=2,
        converters={"SOC 2010 code": str},
    )
    return green_timeshares


def load_onet_green_topics() -> pd.DataFrame():
    """
    Load ONETS green topics per occupation (US SOC)
    """
    green_topics = pd.read_csv(
        "s3://prinz-green-jobs/inputs/data/occupation_data/onet/Occupations_for_all_green_topics.csv",
    )
    return green_topics
