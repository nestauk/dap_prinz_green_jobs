"""
These functions used to be in process_ojo_green_measures, but since they take so long, we pull out to be run as a one off.

We want a dataframe where every row is a skill entity, and it says which job advert it came from and whether its green or not, and the esco ID.


'outputs/data/ojo_application/extracted_green_measures/20240220/all_ojo_large_sample_skills_green_measures_production_True.csv'

Looks like:

>>> skill_measures_df.columns
['job_id', 'NUM_ORIG_ENTS', 'NUM_SPLIT_ENTS', 'ENTS', 'GREEN_ENTS', 'PROP_GREEN', 'BENEFITS']



>>> skill_measures_df[0].to_dicts()
[{'job_id': 42782671,
'NUM_ORIG_ENTS': 28,
'NUM_SPLIT_ENTS': 28,
'ENTS': '[[[\'can-do positive attitude\'], \'SKILL\'], ..., [[\'Experience and accreditation of an industry recognised project management qualification\'], \'EXPERIENCE\']]',
'GREEN_ENTS': '[]',
'PROP_GREEN': 0.0,
'BENEFITS': "['Directors C-Level contacts', '25 days holiday', 'Quarterly Socials']"}
]


We want two outputs:
1. dataframe where each row is a job ad and it says the num entities and prop green skills
2. dataframe where every row is a green skill found in a job advert and the green esco ID. (we have this equivalent not-green data from ojo run)


"""

from dap_prinz_green_jobs import BUCKET_NAME, logger, analysis_config
from dap_prinz_green_jobs.getters.data_getters import (
    load_s3_data,
    get_s3_data_paths,
    save_to_s3,
)
from dap_prinz_green_jobs.getters.industry_getters import load_sic
from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupations_measures_utils import (
    OccupationMeasures,
)
from dap_prinz_green_jobs.utils.processing import list_chunks
from dap_prinz_green_jobs.analysis.ojo_analysis.process_ojo_green_measures import (
    read_process_taxonomies,
)

import pandas as pd
import polars as pl
import numpy as np
from tqdm import tqdm

from typing import Tuple, Dict, Union
import ast
import re
from collections import defaultdict

#####################

latest_all_skills_date_stamp = "20241118"

latest_all_skills_df_path = f"s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/{latest_all_skills_date_stamp}/ojo_all_skills_green_measures_production_True.parquet"

ojo_dedupe_date = "20241114"

ojo_dedupe_file = f"s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/{ojo_dedupe_date}/latest_update_{ojo_dedupe_date}_skills.parquet"

#####################

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


def safe_literal_eval(value) -> Union[None, str, int, float, list, dict]:
    """
    Safely evaluate an expression node or a string containing a Python literal or container display.
    """
    try:
        return ast.literal_eval(value)
    except (SyntaxError, ValueError):
        # Handle the exception (e.g., return a default value or NaN)
        return None


def fix_new_green_list_structure(green_ents):
    """
    The newest GREEN_ENTS data is contained within an extra set of brackets, so remove these
    """
    if str(green_ents).startswith("[[["):
        return green_ents[0]
    else:
        return green_ents


if __name__ == "__main__":
    logger.info("Loading the green skills extracted data")
    skill_measures_df = pl.read_parquet(latest_all_skills_df_path)

    green_skill_id_2_name, full_skill_id_2_name = read_process_taxonomies()

    logger.info(
        "Get all the skills mappings from when the skills extraction algorithm was applied in the OJO dataset"
    )

    all_skills_data = pl.read_parquet(ojo_dedupe_file)
    num_all_skills = all_skills_data[
        "id"
    ].value_counts()  # How many ESCO skills per job advert
    num_all_skills = num_all_skills.with_columns(
        pl.col("id").cast(pl.Int64).alias("id"),
    ).rename({"count": "num_all_skills_ojo"})

    skill_measures_df = skill_measures_df.with_columns(
        pl.col("job_id").cast(pl.Int64).alias("job_id")
    )

    # Another output
    skill_metrics = skill_measures_df[
        ["job_id", "PROP_GREEN", "NUM_ORIG_ENTS", "NUM_SPLIT_ENTS"]
    ]
    skill_metrics = skill_metrics.join(
        num_all_skills, how="outer", left_on="job_id", right_on="id"
    )

    # Process skills files in batches, otherwise it will crash

    chunk_size = 500000
    logger.info(
        f"Exploding skills for {len(skill_measures_df)} file names in {round(len(skill_measures_df)/chunk_size)} chunks"
    )

    all_green_skills_df = pd.DataFrame()
    count_green_skills = defaultdict(int)
    for skill_measures_df_chunk in tqdm(list_chunks(skill_measures_df, chunk_size)):
        skill_measures_df_chunk = skill_measures_df_chunk.to_pandas()
        skill_measures_df_chunk["GREEN_ENTS"] = skill_measures_df_chunk[
            "GREEN_ENTS"
        ].apply(safe_literal_eval)
        skill_measures_df_chunk["GREEN_ENTS"] = skill_measures_df_chunk[
            "GREEN_ENTS"
        ].apply(lambda x: fix_new_green_list_structure(x))

        green_ents_explode = (
            skill_measures_df_chunk[["job_id", "GREEN_ENTS"]]
            .explode("GREEN_ENTS")
            .reset_index(drop=True)
        )
        green_ents_explode.loc[
            green_ents_explode["GREEN_ENTS"].str.len() == 0, "GREEN_ENTS"
        ] = np.nan
        green_ents_explode["skill_label"] = green_ents_explode["GREEN_ENTS"].apply(
            lambda x: x[0] if (isinstance(x, list)) else None
        )
        green_ents_explode["extracted_green_skill"] = green_ents_explode[
            "GREEN_ENTS"
        ].apply(lambda x: x[1][2][0] if isinstance(x, list) else None)
        green_ents_explode["extracted_green_skill_id"] = green_ents_explode[
            "GREEN_ENTS"
        ].apply(lambda x: x[1][2][1] if isinstance(x, list) else None)
        green_ents_explode = green_ents_explode[
            (
                (green_ents_explode["skill_label"] != "")
                & (pd.notnull(green_ents_explode["skill_label"]))
            )
        ]
        # # Remove the duplicate green skills per job advert
        green_ents_explode.sort_values(by="extracted_green_skill", inplace=True)
        green_ents_explode.drop_duplicates(
            subset=["job_id", "skill_label"], keep="first", inplace=True
        )
        green_ents_explode["green_skill_preferred_name"] = green_ents_explode[
            "extracted_green_skill_id"
        ].map(green_skill_id_2_name)
        green_ents_explode.drop(columns=["GREEN_ENTS"], inplace=True)
        # Convert all h&s skills to not be green
        green_ents_explode = green_ents_explode[
            green_ents_explode["green_skill_preferred_name"]
            != "health and safety regulations"
        ]
        # Add number of green skills to the counter
        for k, v in green_ents_explode["job_id"].value_counts().to_dict().items():
            count_green_skills[k] += v
        all_green_skills_df = pd.concat([all_green_skills_df, green_ents_explode])

    skill_metrics = (
        skill_metrics.with_columns(
            pl.col("job_id")
            .replace_strict(count_green_skills, default=0)
            .alias("count_green_skills_no_hs")
        )
        .rename({"PROP_GREEN": "prop_green_with_hs"})
        .drop("id")
    )
    # Use the green measures number of all entities (otherwise num green can be greater than total num)
    skill_metrics = skill_metrics.with_columns(
        pl.col("count_green_skills_no_hs")
        .truediv(pl.col("NUM_SPLIT_ENTS"))
        .alias("PROP_GREEN"),
    )

    # Save the counts data

    save_to_s3(
        BUCKET_NAME,
        skill_metrics.to_pandas(),
        f"outputs/data/ojo_application/extracted_green_measures/{latest_all_skills_date_stamp}/ojo_all_skills_green_measures_skill_metrics.parquet",
    )

    # Save the green skills data exploded

    save_to_s3(
        BUCKET_NAME,
        all_green_skills_df,
        f"outputs/data/ojo_application/extracted_green_measures/{latest_all_skills_date_stamp}/ojo_all_skills_green_measures_exploded_green.parquet",
    )

    # Save the key columns for the all skills data (helps with loading a smaller dataset in the aggregation step)

    write_polars_s3(
        all_skills_data[["id", "esco_id"]].rename(
            {"id": "job_id", "esco_id": "extracted_full_skill_id"}
        ),
        f"s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/{latest_all_skills_date_stamp}/ojo_all_skills_exploded.parquet",
    )
