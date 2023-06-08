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
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/ojo_application/deduplicated_sample/green_salaries_data_sample.csv",

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


def get_extracted_skill_embeddings() -> Dict[str, List[float]]:
    """
    Gets the extracted skill embeddings (output from get_ojo_skills_sample) from s3
        the extracted skills are from the job sample output from


    Returns:
        dictionary of extracted skill embeddings where the key is the extracted skill
        and the value is a list of floats representing the embedding
    """
    return load_s3_data(
        BUCKET_NAME,
        "outputs/data/green_skill_lists/extracted_skills_embeddings.json",
    )


def get_mapped_green_skills() -> List[Dict[str, list]]:
    """Loads mapped green skills from s3 from random ojo sample

    Returns:
        A list of dictionaries where there is a job_id key and
        a list of mapped green skills based on gm.skill_threshold value
    """
    return load_s3_data(
        BUCKET_NAME, "outputs/data/green_skills/20230526_all_matched_skills.json"
