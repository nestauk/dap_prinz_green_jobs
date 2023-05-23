"""
Utils for occuational green measures
"""

import pandas as pd

from typing import List, Union


def clean_job_title(job_title: str) -> str:
    """Cleans the job title

    :param job_title: A job title
    :type job_title: str

    :return: A cleaned job title
    :rtype: str

    """

    return job_title.lower()


def map_job_title(
    job_title: Union[str, List[str]], soc_job_titles: dict
) -> Union[str, List[str]]:
    """Finds the SOC(s) for a particular job title(s)

    :param job_title: A job title
    :type job_title: str or a list of string

    :param soc_job_titles: SOCs for each job title
    :type soc_job_titles: dict

    :return: A SOC or list of SOCs for the inputted job titles
    :rtype: str or list

    """

    if job_title.isinstance(str):
        return soc_job_titles.get(job_title)
    else:
        return [soc_job_titles.get(title) for title in job_title]


def load_job_title_soc() -> pd.DataFrame():
    """
    Load the dataset which will give SOC codes for each job title
    """

    jobtitle_soc_data = pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/occupation_data/ons/indexsocextv5updated.xlsx",
        sheet_name="SOC 2020 6 Digit Index",
    )

    jobtitle_soc_data["soc_6_2020"] = jobtitle_soc_data["SOC 2020 Ext Code"].astype(
        "str"
    )
    jobtitle_soc_data["soc_4_2020"] = jobtitle_soc_data["SOC 2020"].astype("str")
    jobtitle_soc_data["soc_4_2010"] = jobtitle_soc_data["SOC 2010"].astype("str")

    return jobtitle_soc_data


def load_green_soc() -> pd.DataFrame():
    """
    Load the datasets which gives green measures from SOC codes
    """

    green_gla_data = (
        pd.read_excel(
            "s3://prinz-green-jobs/inputs/data/occupation_data/gla/Summary of green occupations (Nov 2021).xlsx",
            sheet_name="1. List of all occupations",
            skiprows=3,
            converters={"SOC2010 4-digit": str},
        )
        .add_prefix("GLA_")
        .rename(columns={"GLA_SOC2010 4-digit": "SOC_2010"})
    )

    green_timeshares = (
        pd.read_excel(
            "s3://prinz-green-jobs/inputs/data/occupation_data/ons/greentimesharesoc.xlsx",
            sheet_name="SOC_2010",
            skiprows=2,
            converters={"SOC 2010 code": str},
        )
        .add_prefix("timeshare_")
        .rename(columns={"timeshare_SOC 2010 code": "SOC_2010"})
    )

    green_soc_data = pd.merge(
        green_gla_data,
        green_timeshares,
        how="outer",
        on="SOC_2010",
    ).rename(columns={"SOC 2010 code": "SOC_2010"})

    return green_soc_data


def create_job_title_soc_mapper(
    jobtitle_soc_data: pd.DataFrame(),
    job_title_column_name: str = "INDEXOCC NATURAL WORD ORDER",
    soc_column_name: str = "soc_4_2010",
) -> dict:
    """Creates a dictionary from a job title to a SOC

    :param jobtitle_soc_data: A dataframe containing job titles and SOC
    :type jobtitle_soc_data: pd.DataFrame()

    :param job_title_column_name: The column name containing the job title
    :type job_title_column_name: str

    :param soc_column_name: The column name containing the SOC
    :type soc_column_name: str

    :return: A dictionary of job titles to a SOC
    :rtype: dict

    """

    soc_mapper = dict(
        zip(
            jobtitle_soc_data[job_title_column_name], jobtitle_soc_data[soc_column_name]
        )
    )
    soc_mapper = {
        k: v for k, v in soc_mapper.items() if v != "}}}}"
    }  # A nuance of the dataset

    return soc_mapper
