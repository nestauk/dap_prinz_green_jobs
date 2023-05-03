"""
Create a sample from the deduplicated job adverts.

Use this to sample the OJO tables.
Also create a merged "useful" table with descriptions+job title+date for the sample.
"""
import random

import pandas as pd

from dap_prinz_green_jobs.getters.data_getters import save_to_s3
from dap_prinz_green_jobs import BUCKET_NAME, logger, config


def filter_data(data, filter_ids_set, id_col_name="id"):
    filtered_data = data[data[id_col_name].isin(filter_ids_set)]
    filtered_data.reset_index(drop=True, inplace=True)
    return filtered_data


if __name__ == "__main__":
    deduplicated_ids = pd.read_csv(config["ojo_deduplication_file"])

    # Sample job adverts

    logger.info("Creating and saving job id sample")

    random.seed(42)
    sample_ids = random.sample(
        deduplicated_ids["id"].tolist(), config["ojo_random_sample_size"]
    )

    save_to_s3(
        BUCKET_NAME,
        sample_ids,
        f"outputs/data/ojo_application/deduplicated_sample/sampled_job_ids.json",
    )

    # Filter the datasets for this sample

    logger.info("Creating and saving the job titles data sample")

    job_title_data = pd.read_parquet(config["ojo_s3_file_adverts_ojd_daps_extract"])
    # There are duplicate rows with different date formats
    job_title_data = job_title_data.drop_duplicates(
        subset=job_title_data.columns.difference(["created"])
    )

    job_title_data_sample = filter_data(
        job_title_data, set(sample_ids), id_col_name="id"
    )

    save_to_s3(
        BUCKET_NAME,
        job_title_data_sample,
        f"outputs/data/ojo_application/deduplicated_sample/job_title_data_sample.csv",
    )

    logger.info("Creating and saving the salaries data sample")

    salaries_data = pd.read_parquet(config["ojo_s3_file_salaries"])
    # There are rows with NA in the min and max salary
    salaries_data = salaries_data.dropna(subset=["min_annualised_salary"])
    salaries_data = salaries_data.drop_duplicates()

    salaries_data_sample = filter_data(salaries_data, set(sample_ids), id_col_name="id")

    save_to_s3(
        BUCKET_NAME,
        salaries_data_sample,
        f"outputs/data/ojo_application/deduplicated_sample/salaries_data_sample.csv",
    )

    logger.info("Creating and saving the locations data sample")

    locations_data = pd.read_parquet(config["ojo_s3_file_locations"])
    locations_data = locations_data.drop_duplicates()

    locations_data_sample = filter_data(
        locations_data, set(sample_ids), id_col_name="id"
    )

    save_to_s3(
        BUCKET_NAME,
        locations_data_sample,
        f"outputs/data/ojo_application/deduplicated_sample/locations_data_sample.csv",
    )

    logger.info("Creating and saving the skills data sample")

    skills_data = pd.read_parquet(config["ojo_s3_file_skills"])

    skills_data_sample = filter_data(skills_data, set(sample_ids), id_col_name="id")

    save_to_s3(
        BUCKET_NAME,
        skills_data_sample,
        f"outputs/data/ojo_application/deduplicated_sample/skills_data_sample.csv",
    )

    logger.info("Creating and saving the descriptions + main info data sample")

    descriptions_data = pd.read_parquet(config["ojo_s3_file_descriptions"])
    descriptions_data["id"] = descriptions_data["id"].astype(int)
    descriptions_data_sample = filter_data(
        descriptions_data, set(sample_ids), id_col_name="id"
    )

    ojo_data_sample = pd.merge(job_title_data_sample, descriptions_data_sample, on="id")
    ojo_data_sample = pd.merge(
        ojo_data_sample,
        locations_data_sample[["id", "itl_3_code", "itl_3_name"]],
        on="id",
    )

    # Save out the main things that are needed
    ojo_data_sample = ojo_data_sample[
        ["id", "job_title_raw", "created", "description", "itl_3_code", "itl_3_name"]
    ]
    ojo_data_sample.reset_index(drop=True, inplace=True)

    save_to_s3(
        BUCKET_NAME,
        ojo_data_sample,
        f"outputs/data/ojo_application/deduplicated_sample/ojo_sample.csv",
    )
