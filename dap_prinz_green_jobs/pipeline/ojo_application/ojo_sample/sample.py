"""
Script to generate sample of 1,000,000 deduplicated, sampled job ids.

We are taking a random sample per SOC4 per itl2 location.

python dap_prinz_green_jobs/pipeline/ojo_application/ojo_sample/sample.py
"""
import pandas as pd

from dap_prinz_green_jobs import logger, config
from dap_prinz_green_jobs.pipeline.ojo_application.ojo_sample.ojo_sample_utils import (
    get_soc4_codes,
    desired_sample_size,
    random_seed,
)

from dap_prinz_green_jobs.pipeline.green_measures.occupations.soc_map import SOCMapper

from tqdm import tqdm

# load SOC mapper
soc_mapper = SOCMapper()
soc_mapper.load()

if __name__ == "__main__":
    logger.info(
        f"Creating a sample of {desired_sample_size} jobs ids per soc4 code per itl2 location..."
    )

    # load job titles
    logger.info("Loading deduplicated job ids, job title, job locations data...")
    deduplicated_ids = pd.read_csv(
        "s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/deduplicated_job_ids.csv"
    )
    deduplicated_ids_list = deduplicated_ids.id.to_list()

    job_titles = pd.read_parquet(config["ojo_s3_file_adverts_ojd_daps_extract"])
    # There are duplicate rows with different date formats
    job_titles = job_titles.drop_duplicates(
        subset=job_titles.columns.difference(["created"])
    )
    # load job locations
    job_locations = pd.read_parquet(config["ojo_s3_file_locations"])
    job_locations = job_locations.drop_duplicates()

    logger.info("Merging datasets...")
    job_title_locations = (
        pd.merge(job_titles, job_locations, on="id")
        .query("id in @deduplicated_ids_list")
        .query("is_uk == 1")
        .query("is_large_geo == 0")[
            [
                "id",
                "job_title_raw",
                "itl_2_code",
                "itl_2_name",
                "itl_3_code",
                "itl_3_name",
            ]
        ]
        .reset_index(drop=True)
    )

    logger.info("Extracting SOC codes...")
    unique_job_titles = job_title_locations.job_title_raw.unique().tolist()
    logger.info(
        f"there are {len(unique_job_titles)} unique job titles to extract SOC codes for..."
    )

    soc_codes = soc_mapper.get_soc(job_titles=unique_job_titles)
    # get soc4 codes to sample on
    soc4_codes = get_soc4_codes(soc_codes)
    jobtitles2soc = dict(zip(unique_job_titles, soc4_codes))
    job_title_locations["soc4_code"] = job_title_locations.job_title_raw.map(
        jobtitles2soc
    )

    logger.info("get random sample of job ids per soc4 code per itl2 code...")
    group_size = job_title_locations.groupby(["soc4_code", "itl_2_code"]).size()
    sample_frac = desired_sample_size / group_size.sum()

    sampled_ids = list()
    for group, group_df in tqdm(
        job_title_locations.groupby(["soc4_code", "itl_2_code"])
    ):
        sampled_group = group_df.sample(
            frac=sample_frac, random_state=random_seed, replace=True
        )
        sampled_ids.extend(sampled_group.id.to_list())
    sampled_ids = list(set(sampled_ids))

    # save sample
    logger.info(f"Saving sample of {len(sampled_ids)} job ids...")
    (
        deduplicated_ids.query("id in @sampled_ids")
        .reset_index(drop=True)
        .to_csv(
            "s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/deduplicated_sampled_job_ids.csv",
            index=False,
        )
    )
