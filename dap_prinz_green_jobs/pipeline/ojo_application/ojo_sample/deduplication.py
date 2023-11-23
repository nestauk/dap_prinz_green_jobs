"""
A script to deduplicate the OJO dataset.

python dap_prinz_green_jobs/pipeline/ojo_application/ojo_sample/deduplication.py
"""

import pandas as pd
import pyarrow.parquet as pq

from tqdm import tqdm

from dap_prinz_green_jobs.getters.data_getters import save_to_s3
from dap_prinz_green_jobs import BUCKET_NAME, logger, config
from dap_prinz_green_jobs.pipeline.ojo_application.ojo_sample.ojo_sample_utils import (
    short_hash,
    get_deduplicated_job_adverts,
)

from tqdm import tqdm

num_units = config["ojo_deduplication_num_units"]
unit_type = config["ojo_deduplication_unit_type"]

if __name__ == "__main__":
    adverts_ojd_daps_extract = pd.read_parquet(
        config["ojo_s3_file_adverts_ojd_daps_extract"]
    )
    # About 1840 rows are exact duplicates
    adverts_ojd_daps_extract = adverts_ojd_daps_extract.drop_duplicates()

    # Will take a long time to load!
    descriptions = pd.read_parquet(config["ojo_s3_file_descriptions"])

    # For speed: job id to a hash of the description text:
    hash_dict = {}
    for _, row in tqdm(descriptions.iterrows()):
        hash_dict[row["id"]] = short_hash(row["description"])

    # save_to_s3('open-jobs-lake', hash_dict, 'latest_output_tables/descriptions_hash.json')

    # Merge what's needed for the deduplication
    job_adverts = adverts_ojd_daps_extract[["id", "job_location_raw", "created"]]
    job_adverts["description_hash"] = job_adverts["id"].map(hash_dict)

    # Can't do anything with the adverts without description text, so remove these before deduplication and sampling
    job_adverts = job_adverts[job_adverts["description_hash"].notnull()]

    if num_units:
        # If you are using a time window it's a bit more complex
        no_duplicates = get_deduplicated_job_adverts(
            job_adverts,
            num_units=num_units,
            unit_type=unit_type,
            id_col="id",
            date_col="created",
            job_loc_col="job_location_raw",
            description_hash="description_hash",
        )
        output_file_name = f"outputs/data/ojo_application/deduplicated_sample/deduplicated_job_ids_{num_units}{unit_type}_chunks.csv"
    else:
        # Without a time window the deduplication is easy
        no_duplicates = job_adverts.sample(frac=1, random_state=42).drop_duplicates(
            ["job_location_raw", "description_hash"]
        )

        output_file_name = (
            "outputs/data/ojo_application/deduplicated_sample/deduplicated_job_ids.csv"
        )

    logger.info(
        f"{len(job_adverts)} job adverts with a description, deduplicated to {len(no_duplicates)} job adverts"
    )

    no_duplicates.reset_index(drop=True, inplace=True)

    save_to_s3(
        BUCKET_NAME,
        no_duplicates,
        output_file_name,
    )
