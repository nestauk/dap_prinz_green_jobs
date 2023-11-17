"""
Getters for data related to OJO job adverts
"""
import pandas as pd
from dap_prinz_green_jobs import BUCKET_NAME
from dap_prinz_green_jobs.getters.data_getters import load_s3_data

from typing import Dict, List


def get_ojo_sample() -> pd.DataFrame:
    """Gets ojo sample data from s3

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/ojo_sample.csv",
    )


def get_ojo_job_title_sample() -> pd.DataFrame:
    """Gets ojo sample data with job title and sectors information from s3

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/job_title_data_sample.csv",
    )


def get_ojo_location_sample() -> pd.DataFrame:
    """Gets ojo sample data with location information from s3

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/locations_data_sample.csv",
    )


def get_ojo_salaries_sample() -> pd.DataFrame:
    """Gets ojo sample data with salaries information from s3

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/salaries_data_sample.csv",
    )


def get_ojo_skills_sample() -> pd.DataFrame:
    """Gets ojo sample data with skills information from s3

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/skills_data_sample.csv",
    )


## ditto but for "green" jobs based on keyword approach


def get_green_ojo_sample() -> pd.DataFrame:
    """Gets ojo sample data from s3 filtered by green keywords

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/green_ojo_sample.csv",
    )


def get_green_ojo_job_title_sample() -> pd.DataFrame:
    """Gets ojo sample data with job title and sectors information from s3 filtered by green keywords

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/green_job_title_data_sample.csv",
    )


def get_green_ojo_location_sample() -> pd.DataFrame:
    """Gets ojo sample data with location information from s3 filtered by green keywords

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/green_locations_data_sample.csv",
    )


def get_green_ojo_salaries_sample() -> pd.DataFrame:
    """Gets ojo sample data with salaries information from s3 filtered by green keywords

    Returns:
        pd.Dataframe: ojo sample data
    """
    return (
        load_s3_data(
            BUCKET_NAME,
            "outputs/data/ojo_application/deduplicated_sample/green_salaries_data_sample.csv",
        ),
    )


def get_extracted_green_measures() -> Dict[str, Dict[str, List[str]]]:
    """
    Gets the extracted green measures from s3

    Returns:
        dictionary of extracted green measures
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/extracted_green_measures/ojo_sample_green_measures_production_True_base.json",
    )


def get_green_ojo_skills_sample() -> pd.DataFrame:
    """Gets ojo sample data with skills information from s3 filtered by green keywords
    NOTE: they're skills from SkillsExtractor, not any developments from the project

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/green_skills_data_sample.csv",
    )


## ditto but for mixed jobs based on keyword approach + random sample of non-green jobs


def get_mixed_ojo_sample() -> pd.DataFrame:
    """Gets ojo sample data from s3 filtered by green keywords + random sample

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/mixed_ojo_sample.csv",
    )


def get_mixed_ojo_job_title_sample() -> pd.DataFrame:
    """Gets ojo sample data with job title and sectors information from
        s3 filtered by green keywords + random sample

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/mixed_job_title_data_sample.csv",
    )


def get_mixed_ojo_location_sample() -> pd.DataFrame:
    """Gets ojo sample data with location information from s3 filtered
        by green keywords + random sample

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/mixed_locations_data_sample.csv",
    )


def get_mixed_ojo_salaries_sample() -> pd.DataFrame:
    """Gets ojo sample data with salaries information from s3 filtered
        by green keywords + random sample

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/mixed_salaries_data_sample.csv",
    )


def get_mixed_ojo_skills_sample() -> pd.DataFrame:
    """Gets ojo sample data with skills information from s3 filtered by green keywords
        + random sample
    NOTE: they're skills from SkillsExtractor, not any developments from the project

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/mixed_skills_data_sample.csv",
    )


## ditto but for the final large sample of jobs per soc4 code per itl2 code


def get_large_ojo_sample() -> pd.DataFrame:
    """Gets ojo sample data from s3 filtered by random sample
        of jobs per soc4 code per itl2 code

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/large_ojo_sample.parquet",
    )


def get_large_ojo_job_title_sample() -> pd.DataFrame:
    """Gets ojo job title sample data from s3 filtered by random sample
        of jobs per soc4 code per itl2 code

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/large_job_title_data_sample.parquet",
    )


def get_large_ojo_location_sample() -> pd.DataFrame:
    """Gets ojo job locations sample data from s3 filtered by random sample
        of jobs per soc4 code per itl2 code

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/large_locations_data_sample.parquet",
    )


def get_large_ojo_salaries_sample() -> pd.DataFrame:
    """Gets ojo job salaries sample data from s3 filtered by random sample
        of jobs per soc4 code per itl2 code

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/large_salaries_data_sample.parquet",
    )


def get_large_ojo_skills_sample() -> pd.DataFrame:
    """Gets ojo skills sample from s3 filtered by random sample
        of jobs per soc4 code per itl2 code
    NOTE: they're skills from SkillsExtractor, not any developments from the project


    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/large_skills_data_sample.parquet",
    )
