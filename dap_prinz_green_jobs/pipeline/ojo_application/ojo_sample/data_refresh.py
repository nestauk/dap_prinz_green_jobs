"""
Performs two steps when there is new OJO data for green measures to be extracted from
1. Deduplication
2. Saves data in correct format and location for green jobs measures

27/06/25 (May rerun 6,145,517 adverts)
"""

import os
from datetime import datetime

import pandas as pd
import polars as pl
from tqdm import tqdm

from dap_prinz_green_jobs.getters.data_getters import save_to_s3
from dap_prinz_green_jobs import BUCKET_NAME, logger
from dap_prinz_green_jobs.pipeline.ojo_application.ojo_sample.ojo_sample_utils import (
    short_hash,
)

import s3fs


def write_polars_s3(df, destination):
    fs = s3fs.S3FileSystem()
    # write parquet
    if ".csv" in destination:
        with fs.open(destination, mode="wb") as f:
            df.write_csv(f)
    elif ".parquet" in destination:
        with fs.open(destination, mode="wb") as f:
            df.write_parquet(f)
    else:
        print("destination should be a '.csv' or '.parquet'")


if __name__ == "__main__":
    ## Deduplicate on all the new data

    s3_dir_preffix = "s3://open-jobs-lake/latest_output_tables/"
    ojo_s3_file_adverts_ojd_daps_extract = os.path.join(
        s3_dir_preffix, "archive/202506/adverts_ojd_daps_extract.parquet"
    )
    ojo_s3_file_descriptions = os.path.join(
        s3_dir_preffix, "archive/202506/descriptions.parquet"
    )

    # ojo_s3_file_titles = os.path.join(
    #     s3_dir_preffix, "new_adverts/new_ojd_daps_adverts_extract.parquet" # NOT UPDATED
    # )
    ojo_s3_file_titles = None
    ojo_s3_file_locations = os.path.join(
        s3_dir_preffix, "archive/202506/locations_ojd_daps_extract.parquet"
    )
    ojo_s3_file_salaries = os.path.join(
        s3_dir_preffix, "archive/202506/salaries_ojd_daps_extract.parquet"
    )
    ojo_s3_file_skills = os.path.join(
        s3_dir_preffix, "archive/202506/skills_ojd_daps_extract.parquet"
    )

    previous_deduplication_file = "s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/deduplicated_job_ids.csv"

    # All the concatenated data previous to this update

    previous_skills_file = "s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/latest_update_20241114_skills.parquet"

    previous_titles_file = "s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/latest_update_20241114_titles.parquet"

    previous_locs_file = "s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/latest_update_20241114_locations.parquet"

    previous_salaries_file = "s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/latest_update_20241114_salaries.parquet"

    previous_key_columns_file = "s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/latest_update_20241114_key_columns.parquet"

    previous_desc_file = "s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/latest_update_20241114_descriptions.parquet"

    today = datetime.now().strftime("%Y%m%d")

    output_path = f"outputs/data/ojo_application/deduplicated_sample/{today}"

    logger.info("Load descriptions for deduplication step")

    adverts_ojd_daps_extract = pl.read_parquet(
        ojo_s3_file_adverts_ojd_daps_extract,
        storage_options={
            "aws_region": "eu-west-1"
        },  # This is needed for parquet files in this s3 bucket (not needed for csvs weirdly!)
    )

    # Get rid exact duplicates
    adverts_ojd_daps_extract = adverts_ojd_daps_extract.unique()

    # Will take a long time to load!
    descriptions_data = pl.read_parquet(
        ojo_s3_file_descriptions, storage_options={"aws_region": "eu-west-1"}
    )

    logger.info("Hashing descriptions")

    # For speed: job id to a hash of the description text:
    hash_dict = {}
    for row in tqdm(descriptions_data.iter_rows(named=True)):
        hash_dict[row["id"]] = short_hash(row["description"])

    # Merge what's needed for the deduplication
    job_adverts = adverts_ojd_daps_extract[["id", "job_location_raw", "created"]]
    job_adverts = job_adverts.with_columns(
        pl.col("id").replace_strict(hash_dict).alias("description_hash")
    )

    job_adverts = job_adverts.with_columns(
        pl.col("created").str.strptime(pl.Datetime, format="%d/%m/%Y").alias("created")
    )

    # Find the cut-off date from the old data
    old_data_meta = pl.read_csv(previous_deduplication_file)
    # last_old_data = old_data_meta["created"].str.to_datetime("%Y-%m-%d").max()

    old_data_meta = old_data_meta.with_columns(
        pl.col("created")
        .str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%.f")
        .alias("created")
    )
    last_old_data = old_data_meta["created"].max()

    # Filter the new data to be after this point
    job_adverts_new = job_adverts.filter(pl.col("created") > last_old_data)

    # Can't do anything with the adverts without description text, so remove these before deduplication and sampling
    job_adverts_new = job_adverts_new.drop_nulls(subset="description_hash")

    logger.info("Deduplicating")
    # Deduplicate randomly
    no_duplicates = job_adverts_new.sample(fraction=1, shuffle=True, seed=1).unique(
        subset=["job_location_raw", "description_hash"]
    )

    logger.info(
        f"{len(job_adverts_new)} new job adverts with a description, deduplicated to {len(no_duplicates)} job adverts"
    )

    # # 1726011 new job adverts with a description, deduplicated to 1313447 job adverts - Nov 2024
    # # 205390 new job adverts with a description, deduplicated to 178288 job adverts - May 2025

    write_polars_s3(
        no_duplicates,
        os.path.join("s3://" + BUCKET_NAME, output_path, "deduplicated_job_ids.csv"),
    )

    deduplicated_ids_list = set(no_duplicates["id"].to_list())

    logger.info("Filtering descriptions data on new deduplicated ids")

    descriptions_data_filt = descriptions_data.filter(
        pl.col("id").is_in(deduplicated_ids_list)
    )

    write_polars_s3(
        descriptions_data_filt,
        os.path.join(
            "s3://" + BUCKET_NAME, output_path, "all_ojo_descriptions.parquet"
        ),
    )

    logger.info("Loading and filtering title data on new deduplicated ids")

    # no_duplicates = pl.read_csv(os.path.join("s3://"+BUCKET_NAME, output_path, "deduplicated_job_ids.csv"))
    # deduplicated_ids_list = set(no_duplicates['id'].to_list())

    if ojo_s3_file_titles:
        # In the Nov 24 update the latest data (titles, created, location) was in ojo_s3_file_titles
        # And all the data (same columns) was in ojo_s3_file_adverts_ojd_daps_extract
        # At this time this didn't really need to be read in twice
        # since ojo_s3_file_titles was a subset of ojo_s3_file_adverts_ojd_daps_extract
        # But keeping this in for continuity
        all_titles = pl.read_parquet(
            ojo_s3_file_titles, storage_options={"aws_region": "eu-west-1"}
        )
        titles_data_filt = all_titles.filter(pl.col("id").is_in(deduplicated_ids_list))
    else:
        # The title info is in ojo_s3_file_adverts_ojd_daps_extract which was already read in
        titles_data_filt = adverts_ojd_daps_extract.filter(
            pl.col("id").is_in(deduplicated_ids_list)
        )

    write_polars_s3(
        titles_data_filt,
        os.path.join("s3://" + BUCKET_NAME, output_path, "all_job_title_data.parquet"),
    )

    logger.info("Loading and filtering locations data on new deduplicated ids")

    all_locations = pl.read_parquet(
        ojo_s3_file_locations, storage_options={"aws_region": "eu-west-1"}
    )
    locations_data_filt = all_locations.filter(
        pl.col("id").is_in(deduplicated_ids_list)
    )

    write_polars_s3(
        locations_data_filt,
        os.path.join("s3://" + BUCKET_NAME, output_path, "all_locations_data.parquet"),
    )

    logger.info("Loading and filtering salaries data on new deduplicated ids")

    all_salaries = pl.read_parquet(
        ojo_s3_file_salaries, storage_options={"aws_region": "eu-west-1"}
    )
    salaries_data_filt = all_salaries.filter(pl.col("id").is_in(deduplicated_ids_list))

    write_polars_s3(
        salaries_data_filt,
        os.path.join("s3://" + BUCKET_NAME, output_path, "all_salaries_data.parquet"),
    )

    logger.info("Loading and filtering skills data on new deduplicated ids")

    all_skills = pl.read_parquet(
        ojo_s3_file_skills, storage_options={"aws_region": "eu-west-1"}
    )
    skills_data_filt = all_skills.filter(pl.col("id").is_in(deduplicated_ids_list))

    write_polars_s3(
        skills_data_filt,
        os.path.join("s3://" + BUCKET_NAME, output_path, "all_skills_data.parquet"),
    )

    # Merge key columns together for the combined data

    main_columns_data = descriptions_data_filt[["id", "description"]].join(
        locations_data_filt[["id", "itl_3_code", "itl_3_name"]], on="id", how="inner"
    )
    main_columns_data = main_columns_data.join(
        titles_data_filt[["id", "job_title_raw", "created"]], on="id", how="inner"
    )

    write_polars_s3(
        main_columns_data[
            [
                "id",
                "job_title_raw",
                "created",
                "description",
                "itl_3_code",
                "itl_3_name",
            ]
        ],
        os.path.join("s3://" + BUCKET_NAME, output_path, "all_key_columns.parquet"),
    )

    # Concat the old and the new descriptions datasets into one

    old_desc_data = pl.read_parquet(previous_desc_file)

    all_desc_data = pl.concat(
        [old_desc_data, main_columns_data[["id", "description"]]],
        how="vertical_relaxed",
    )

    write_polars_s3(
        all_desc_data,
        os.path.join(
            "s3://" + BUCKET_NAME,
            output_path,
            f"latest_update_{today}_descriptions.parquet",
        ),
    )

    # Concat the old and the new all_key_columns datasets into one - handy!

    old_main_columns_data = pl.read_parquet(previous_key_columns_file)

    key_columns_data = main_columns_data[
        [
            "id",
            "job_title_raw",
            "created",
            "itl_3_code",
            "itl_3_name",
        ]
    ]

    key_columns_data = key_columns_data.with_columns(
        pl.col("created").str.strptime(pl.Datetime, format="%d/%m/%Y").alias("created")
    )

    all_main_columns_data = pl.concat(
        [old_main_columns_data, key_columns_data], how="vertical_relaxed"
    )

    write_polars_s3(
        all_main_columns_data[
            ["id", "job_title_raw", "created", "itl_3_code", "itl_3_name"]
        ],
        os.path.join(
            "s3://" + BUCKET_NAME,
            output_path,
            f"latest_update_{today}_key_columns.parquet",
        ),
    )

    # Concat the skills data (needed for rest of pipeline)

    new_skills_data = pl.read_parquet(
        f"s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/{today}/all_skills_data.parquet"
    )
    old_skills_data = pl.read_parquet(previous_skills_file)

    all_skills_data = pl.concat(
        [old_skills_data, new_skills_data], how="vertical_relaxed"
    )

    write_polars_s3(
        all_skills_data,
        os.path.join(
            "s3://" + BUCKET_NAME, output_path, f"latest_update_{today}_skills.parquet"
        ),
    )

    # Concat the titles data (needed for rest of pipeline)

    new_titles_data = pl.read_parquet(
        f"s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/{today}/all_job_title_data.parquet"
    )
    old_titles_data = pl.read_parquet(previous_titles_file)

    all_titles_data = pl.concat(
        [old_titles_data, new_titles_data], how="vertical_relaxed"
    )

    write_polars_s3(
        all_titles_data,
        os.path.join(
            "s3://" + BUCKET_NAME, output_path, f"latest_update_{today}_titles.parquet"
        ),
    )

    # Concat the locations data (needed for rest of pipeline)

    new_locations_data = pl.read_parquet(
        f"s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/{today}/all_locations_data.parquet"
    )
    old_locations_data = pl.read_parquet(previous_locs_file)

    all_locations_data = pl.concat(
        [old_locations_data, new_locations_data], how="vertical_relaxed"
    )

    write_polars_s3(
        all_locations_data,
        os.path.join(
            "s3://" + BUCKET_NAME,
            output_path,
            f"latest_update_{today}_locations.parquet",
        ),
    )

    # Concat the salaries data (needed for rest of pipeline)

    new_salaries_data = pl.read_parquet(
        f"s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/{today}/all_salaries_data.parquet"
    )
    old_salaries_data = pl.read_parquet(previous_salaries_file)

    all_salaries_data = pl.concat(
        [old_salaries_data, new_salaries_data], how="vertical_relaxed"
    )

    write_polars_s3(
        all_salaries_data,
        os.path.join(
            "s3://" + BUCKET_NAME,
            output_path,
            f"latest_update_{today}_salaries.parquet",
        ),
    )
