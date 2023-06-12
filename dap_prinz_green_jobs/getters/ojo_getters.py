"""
Getters for data related to OJO job adverts
"""
import pandas as pd
from dap_prinz_green_jobs import BUCKET_NAME
from dap_prinz_green_jobs.getters.data_getters import load_s3_data


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
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/green_salaries_data_sample.csv",
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
