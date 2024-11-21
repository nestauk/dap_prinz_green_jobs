"""
Run the occupation measures for the large sample of job adverts.

python -i dap_prinz_green_jobs/pipeline/ojo_application/flows/ojo_occupation_measures_update.py


"""
from tqdm import tqdm

import os
from toolz import partition_all
from datetime import datetime as date

from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupations_measures_utils import (
    OccupationMeasures,
)
from dap_prinz_green_jobs import config, BUCKET_NAME

from dap_prinz_green_jobs.getters.data_getters import (
    save_to_s3,
    get_s3_data_paths,
    load_s3_data,
)

import pandas as pd
import polars as pl

## ---- Change these everytime you update the data -----

# The existing green measures (no need to run occupation greenness again for these job adverts)
green_occ_existing_data_dir = "s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/20240313/ojo_large_sample_occupation_green_measures_production_true.csv"

# The latest job title data
new_ojo_job_titles_dir = "s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/all_job_title_data.parquet"

## ----------------------------------------------------

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
    production = True
    job_title_column = "job_title_raw"
    id_column = "id"

    date_stamp = str(date.today().date()).replace("-", "")
    folder_name = f"outputs/data/ojo_application/extracted_green_measures/{date_stamp}/"

    om = OccupationMeasures()
    om.load(
        local=config["occupations"]["local"],
        embeddings_output_dir=config["occupations"]["embeddings_output_dir"],
        batch_size=config["occupations"]["batch_size"],
        match_top_n=config["occupations"]["match_top_n"],
        sim_threshold=config["occupations"]["sim_threshold"],
        top_n_sim_threshold=config["occupations"]["top_n_sim_threshold"],
        minimum_n=config["occupations"]["minimum_n"],
        minimum_prop=config["occupations"]["minimum_prop"],
        save_embeds=config["occupations"]["save_embeds"],
    )

    soc_name_dict = {
        "soc_2020_6": om.soc_mapper.soc_2020_6_dict,
        "soc_2020_4": om.soc_mapper.soc_2020_4_dict,
    }

    save_to_s3(
        BUCKET_NAME,
        soc_name_dict,
        os.path.join(folder_name, f"soc_name_dict.json"),
    )

    if not production:
        test_sample_n = 10
        chunk_size = 5  # Number of job titles in each extract_soc step
    else:
        chunk_size = 10000

    print("loading datasets...")

    green_occ_existing_data = pl.read_csv(green_occ_existing_data_dir)

    # Remove any job adverts which have existing green measures (there shouldn't really be any)
    all_ojo_titles = pl.read_parquet(new_ojo_job_titles_dir)
    existing_ids = set(green_occ_existing_data["job_id"].to_list())
    new_ojo_titles = all_ojo_titles.filter(~pl.col("id").is_in(existing_ids))

    print(
        f"There are {len(new_ojo_titles)} job adverts without existing green industry measures (of which there are {len(existing_ids)})"
    )

    # The format used in OccupationMeasures
    ojo_jobs_data = new_ojo_titles[[id_column, job_title_column]].to_dicts()

    if not production:
        ojo_jobs_data = ojo_jobs_data[:test_sample_n]

    unique_job_titles = list(
        set(
            [
                job.get(job_title_column)
                for job in ojo_jobs_data
                if job_title_column in job
            ]
        )
    )

    print(
        f"there are {len(unique_job_titles)} unique job titles to extract SOC codes for..."
    )

    job_title_chunks = list(partition_all(chunk_size, unique_job_titles))

    print(
        f".. finding SOC information for these in {len(job_title_chunks)} batches of {chunk_size} job titles each."
    )

    print("Extract SOC codes for unique job titles")

    for i, job_title_chunk in tqdm(enumerate(job_title_chunks)):
        job_title_2_match = om.precalculate_soc_mapper(job_title_chunk)
        save_to_s3(
            BUCKET_NAME,
            job_title_2_match,
            os.path.join(
                folder_name,
                f"ojo_newest_jobtitles2soc_production_{str(production).lower()}/{i}.json",
            ),
        )

    job_title_2_match_locs = get_s3_data_paths(
        BUCKET_NAME,
        os.path.join(
            folder_name,
            f"ojo_newest_jobtitles2soc_production_{str(production).lower()}",
        ),
        file_types=["*.json"],
    )

    print("Load job title to SOC")
    all_job_title_2_match = {}
    for job_title_2_match_loc in tqdm(job_title_2_match_locs):
        all_job_title_2_match.update(load_s3_data(BUCKET_NAME, job_title_2_match_loc))

    om.job_title_2_match = all_job_title_2_match

    job_ad_chunks = list(partition_all(chunk_size, ojo_jobs_data))

    print(
        f"Finding green measures information for {len(job_ad_chunks)} batches of {chunk_size} job adverts each."
    )

    all_green_occupation_measures_dict = {}

    for job_ad_chunk in tqdm(job_ad_chunks):
        occ_green_measures_list = om.get_measures(
            job_adverts=job_ad_chunk, job_title_key=job_title_column
        )
        green_occupation_measures_dict = dict(
            zip([j[id_column] for j in job_ad_chunk], occ_green_measures_list)
        )
        all_green_occupation_measures_dict.update(green_occupation_measures_dict)

    print("saving to s3...")

    save_to_s3(
        BUCKET_NAME,
        all_green_occupation_measures_dict,
        os.path.join(
            folder_name,
            f"ojo_newest_occupation_green_measures_production_{str(production).lower()}.json",
        ),
    )

    # make parquet file and save to s3
    occs_measures_df = (
        pd.DataFrame.from_dict(all_green_occupation_measures_dict, orient="index")
        .reset_index()
        .rename(columns={"index": "job_id"})
    )

    occs_df_path = os.path.join(
        BUCKET_NAME,
        folder_name,
        f"ojo_newest_occupation_green_measures_production_{str(production).lower()}.parquet",
    )
    occs_measures_df.to_parquet(f"s3://{occs_df_path}", index=False)

    # Join with the existing green occupation measures

    green_occ_existing_data_pd = green_occ_existing_data.to_pandas()

    all_occs_measures_df = pd.concat([green_occ_existing_data_pd, occs_measures_df])

    occs_all_df_path = os.path.join(
        folder_name,
        f"ojo_all_occupation_green_measures_production_{production}.parquet",
    )
    all_occs_measures_df["SOC"] = all_occs_measures_df["SOC"].astype(str)

    save_to_s3(BUCKET_NAME, all_occs_measures_df, occs_all_df_path)
