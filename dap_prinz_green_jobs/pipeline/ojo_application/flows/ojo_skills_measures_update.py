"""
Run the skills measures for the just new job adverts.

python dap_prinz_green_jobs/pipeline/ojo_application/flows/ojo_skills_measures_update.py

- filter new job advert data to not include any jobs that already have green skills measures
- calculate green skills measures for new data
- merge with original green skills data

"""

from dap_prinz_green_jobs import logger
from dap_prinz_green_jobs.getters.data_getters import (
    save_to_s3,
    get_s3_data_paths,
    load_s3_data,
)
from dap_prinz_green_jobs import BUCKET_NAME, config
from dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils import (
    SkillMeasures,
)
from dap_prinz_green_jobs.getters.ojo_getters import (
    get_large_ojo_sample,
)

from toolz import partition_all

from tqdm import tqdm
import pandas as pd
import polars as pl

from argparse import ArgumentParser
from datetime import datetime as date

import os
import numpy as np


## ---- Change these everytime you update the data -----

# The existing green measures (no need to run skills greenness again for these job adverts)
green_skills_existing_data_dir = "s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/20240220/all_ojo_large_sample_skills_green_measures_production_True.csv"

# The latest job advert data
new_ojo_descriptions_dir = "s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/all_ojo_descriptions.parquet"

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
    parser = ArgumentParser()
    parser.add_argument("--production", action="store_true", default=False)
    parser.add_argument("--job_desc_column", default="description", type=str)
    parser.add_argument("--id_column", default="id", type=str)
    parser.add_argument("--test_n", default=100, type=int)

    args = parser.parse_args()
    production = args.production
    id_column = args.id_column
    test_n = args.test_n
    job_desc_column = args.job_desc_column

    if not production:
        chunk_size = 20
    else:
        chunk_size = 10000

    print("loading datasets...")

    green_skills_existing_data = pl.read_csv(green_skills_existing_data_dir)

    # Remove any job adverts which have existing green measures (there shouldn't really be any)
    all_ojo_descriptions = pl.read_parquet(new_ojo_descriptions_dir)
    existing_ids = set(green_skills_existing_data["job_id"].to_list())
    new_ojo_descriptions = all_ojo_descriptions.filter(
        ~pl.col("id").is_in(existing_ids)
    )

    print(
        f"There are {len(new_ojo_descriptions)} job adverts without existing green skills measures (of which there are {len(existing_ids)})"
    )

    # The format used in SkillMeasures
    ojo_jobs_data = (
        new_ojo_descriptions[[id_column, job_desc_column]]
        .rename(
            {
                id_column: config["job_adverts"]["job_id_key"],
                job_desc_column: config["job_adverts"]["job_text_key"],
            }
        )
        .to_dicts()
    )

    if not production:
        ojo_jobs_data = ojo_jobs_data[:test_n]

    date_stamp = str(date.today().date()).replace("-", "")
    folder_name = f"outputs/data/ojo_application/extracted_green_measures/{date_stamp}/"

    skills_output_folder = f"outputs/data/green_skill_lists/{date_stamp}"

    # Skills config variables
    skills_config_name = config["skills"]["skills_config_name"]
    load_skills = config["skills"][
        "load_skills"
    ]  # Set to false if your job adverts or NER model changes
    load_skills_embeddings = config["skills"][
        "load_skills_embeddings"
    ]  # Set to false if your job advert data, NER model or way to embed changes
    load_taxonomy_embeddings = config["skills"][
        "load_taxonomy_embeddings"
    ]  # Set to false if your input taxonomy data or way to embed changes
    green_skills_classifier_model_file_name = config["skills"][
        "green_skills_classifier_model_file_name"
    ]

    if config["skills"]["load_taxonomy_embeddings"]:
        green_tax_embedding_path = config["skills"]["green_tax_embedding_path"]
    else:
        green_tax_embedding_path = os.path.join(
            skills_output_folder, "green_esco_embeddings.json"
        )

    sm = SkillMeasures(
        config_name="extract_green_skills_esco",
        green_skills_classifier_model_file_name=green_skills_classifier_model_file_name,
    )
    sm.initiate_extract_skills(local=False, verbose=True)

    taxonomy_skills_embeddings_dict = sm.get_green_taxonomy_embeddings(
        output_path=green_tax_embedding_path,
        load=load_taxonomy_embeddings,
    )

    job_desc_chunks = list(partition_all(chunk_size, ojo_jobs_data))

    print(
        f"Finding skills information for {chunk_size} job adverts in {len(job_desc_chunks)} batches."
    )

    for i, job_desc_chunk in tqdm(enumerate(job_desc_chunks)):
        skills_output = os.path.join(
            skills_output_folder, f"predicted_skills_production_{production}/{i}.json"
        )
        skill_embeddings_output = os.path.join(
            skills_output_folder,
            f"extracted_skills_embeddings_production_{production}/{i}.json",
        )

        # Where to output the mappings of skills to all of ESCO (not just green)
        skill_mappings_output_path = os.path.join(
            skills_output_folder,
            f"full_esco_skill_mappings_production_{production}/{i}.json",
        )

        prop_green_skills = sm.get_measures(
            job_desc_chunk,
            skills_output_path=skills_output,
            load_skills=load_skills,
            job_text_key=config["job_adverts"]["job_text_key"],
            job_id_key=config["job_adverts"]["job_id_key"],
            skill_embeddings_output_path=skill_embeddings_output,
            load_skills_embeddings=load_skills_embeddings,
            skill_mappings_output_path=skill_mappings_output_path,
        )

        save_to_s3(
            BUCKET_NAME,
            prop_green_skills,
            os.path.join(
                skills_output_folder,
                f"ojo_newest_skills_green_measures_production_{production}_interim/{i}.json",
            ),
        )

    # Read them back in and save altogether
    prop_green_skills_locs = get_s3_data_paths(
        BUCKET_NAME,
        os.path.join(
            skills_output_folder,
            f"ojo_newest_skills_green_measures_production_{production}_interim",
        ),
        file_types=["*.json"],
    )

    print("Load green measures per job advert")
    all_prop_green_skills = {}
    for prop_green_skills_loc in tqdm(prop_green_skills_locs):
        all_prop_green_skills.update(load_s3_data(BUCKET_NAME, prop_green_skills_loc))

    save_to_s3(
        BUCKET_NAME,
        all_prop_green_skills,
        os.path.join(
            folder_name,
            f"ojo_newest_skills_green_measures_production_{production}.json",
        ),
    )

    skill_measures_df = (
        pd.DataFrame.from_dict(all_prop_green_skills, orient="index")
        .reset_index()
        .rename(columns={"index": "job_id"})
    )
    # save as csv because of invalid parquet schema
    skills_df_path = os.path.join(
        BUCKET_NAME,
        folder_name,
        f"ojo_newest_skills_green_measures_production_{production}.csv",
    )

    skill_measures_df["ENTS"] = skill_measures_df["ENTS"].astype(str)
    skill_measures_df.to_parquet(f"s3://{skills_df_path}", index=False)

    # Join with the existing green skills measures

    skill_measures_pl = pl.from_pandas(skill_measures_df)

    skill_measures_pl = skill_measures_pl.with_columns(
        pl.format(
            "[{}]", pl.col("GREEN_ENTS").cast(pl.List(pl.String)).list.join(", ")
        ).alias("GREEN_ENTS")
    )
    skill_measures_pl = skill_measures_pl.with_columns(
        pl.format(
            "[{}]", pl.col("BENEFITS").cast(pl.List(pl.String)).list.join(", ")
        ).alias("BENEFITS")
    )

    # skill_measures_pl = skill_measures_pl.with_columns(pl.col("BENEFITS").cast(pl.String, strict=False))

    all_skills_measures_df = pl.concat(
        [green_skills_existing_data, skill_measures_pl], how="vertical_relaxed"
    )

    skills_all_df_path = os.path.join(
        BUCKET_NAME,
        folder_name,
        f"ojo_all_skills_green_measures_production_{production}.csv",
    )
    write_polars_s3(all_skills_measures_df, f"s3://{skills_all_df_path}")
    write_polars_s3(
        all_skills_measures_df, f"s3://{skills_all_df_path.replace('.csv', '.parquet')}"
    )
