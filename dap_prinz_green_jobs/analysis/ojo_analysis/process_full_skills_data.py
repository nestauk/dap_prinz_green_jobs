"""
These functions used to be in process_ojo_green_measures, but since they take so long, we pull out to be run as a one off
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
import numpy as np
from tqdm import tqdm

from typing import Tuple, Dict, Union
import ast
import re


def safe_literal_eval(value) -> Union[None, str, int, float, list, dict]:
    """
    Safely evaluate an expression node or a string containing a Python literal or container display.
    """
    try:
        return ast.literal_eval(value)
    except (SyntaxError, ValueError):
        # Handle the exception (e.g., return a default value or NaN)
        return None


def load_full_skill_mapping(analysis_config: Dict[str, str]) -> Dict[str, str]:
    full_skill_mapping_dir = f"outputs/data/green_skill_lists/{analysis_config['skills_date_stamp']}/full_esco_skill_mappings_production_{analysis_config['production']}/"
    file_names = get_s3_data_paths(
        BUCKET_NAME, full_skill_mapping_dir, file_types=["*.json"]
    )
    logger.info(f"Loading full skills mappings to ESCO from {len(file_names)} S3 files")
    full_skill_mapping = {}
    for file_name in tqdm(file_names):
        full_skill_mapping.update(load_s3_data(BUCKET_NAME, file_name))
    return full_skill_mapping


def create_skill_df(
    skill_measures_data: pd.DataFrame,
    full_skill_mapping: dict,
    green_skill_id_2_name,
    full_skill_id_2_name,
    job_id_col: str = "job_id",
    skill_match_thresh: float = 0.7,
) -> pd.DataFrame:
    """
    Process the skills measures dataframe where each row is a job advert, into a format
    where each row is a skill and there is information about which job advert it was found in,
    whether it is green or not, and which esco skill it maps to.
    """
    skill_measures_data["GREEN_ENTS"] = skill_measures_data["GREEN_ENTS"].apply(
        safe_literal_eval
    )
    skill_measures_data["ENTS"] = skill_measures_data["ENTS"].apply(safe_literal_eval)
    ents_explode = (
        skill_measures_data[[job_id_col, "ENTS"]].explode("ENTS").reset_index(drop=True)
    )
    ents_explode["skill_label"] = ents_explode["ENTS"].apply(
        lambda x: x[0] if x else []
    )
    ents_explode = ents_explode.explode("skill_label").reset_index(drop=True)
    extracted_full_skill = []
    extracted_full_skill_id = []
    for skill_label in tqdm(ents_explode["skill_label"]):
        full_skills_output = full_skill_mapping.get(skill_label)
        if full_skills_output and full_skills_output[2] >= skill_match_thresh:
            extracted_full_skill.append(full_skills_output[0])
            extracted_full_skill_id.append(full_skills_output[1])
        else:
            extracted_full_skill.append(None)
            extracted_full_skill_id.append(None)
    ents_explode["extracted_full_skill"] = extracted_full_skill
    ents_explode["extracted_full_skill_id"] = extracted_full_skill_id
    green_ents_explode = (
        skill_measures_data[[job_id_col, "GREEN_ENTS"]]
        .explode("GREEN_ENTS")
        .reset_index(drop=True)
    )
    green_ents_explode["skill_label"] = green_ents_explode["GREEN_ENTS"].apply(
        lambda x: x[0] if isinstance(x, list) else None
    )
    green_ents_explode["extracted_green_skill"] = green_ents_explode[
        "GREEN_ENTS"
    ].apply(lambda x: x[1][2][0] if isinstance(x, list) else None)
    green_ents_explode["extracted_green_skill_id"] = green_ents_explode[
        "GREEN_ENTS"
    ].apply(lambda x: x[1][2][1] if isinstance(x, list) else None)
    green_skills_df = pd.concat([ents_explode, green_ents_explode])
    green_skills_df = green_skills_df[
        (
            (green_skills_df["skill_label"] != "")
            & (pd.notnull(green_skills_df["skill_label"]))
        )
    ]
    # # Remove the duplicate green skills per job advert
    green_skills_df.sort_values(by="extracted_green_skill", inplace=True)
    green_skills_df.drop_duplicates(
        subset=[job_id_col, "skill_label"], keep="first", inplace=True
    )
    green_skills_df["full_skill_preferred_name"] = green_skills_df[
        "extracted_full_skill_id"
    ].map(full_skill_id_2_name)
    green_skills_df["green_skill_preferred_name"] = green_skills_df[
        "extracted_green_skill_id"
    ].map(green_skill_id_2_name)
    green_skills_df.drop(columns=["ENTS", "GREEN_ENTS"], inplace=True)
    return green_skills_df


if __name__ == "__main__":
    logger.info("Loading skills data")
    skill_measures_df = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/ojo_application/extracted_green_measures/{analysis_config['skills_date_stamp']}/{analysis_config['skills_file_name']}",
    )

    green_skill_id_2_name, full_skill_id_2_name = read_process_taxonomies()

    full_skill_mapping = load_full_skill_mapping(analysis_config)

    # Process skills files in batches, otherwise it will crash

    chunk_size = 500000
    logger.info(
        f"Exploding skills for {len(skill_measures_df)} file names in {round(len(skill_measures_df)/chunk_size)} chunks"
    )

    all_green_skills_df = pd.DataFrame()
    for skill_measures_df_chunk in tqdm(list_chunks(skill_measures_df, chunk_size)):
        green_skills_df_chunk = create_skill_df(
            skill_measures_df_chunk,
            full_skill_mapping,
            green_skill_id_2_name,
            full_skill_id_2_name,
        )
        all_green_skills_df = pd.concat([all_green_skills_df, green_skills_df_chunk])

    save_to_s3(
        BUCKET_NAME,
        all_green_skills_df,
        f"outputs/data/ojo_application/extracted_green_measures/{analysis_config['skills_date_stamp']}/exploded_{analysis_config['skills_file_name']}",
    )

    # all_green_skills_df is 5.2 GB, but we don't always need all the columns, so just leave the ones needed for process_ojo_green_measures.py

    all_green_skills_df_essential = all_green_skills_df[
        ["job_id", "extracted_full_skill_id", "extracted_green_skill_id"]
    ]

    save_to_s3(
        BUCKET_NAME,
        all_green_skills_df_essential,
        f"outputs/data/ojo_application/extracted_green_measures/{analysis_config['skills_date_stamp']}/exploded_essential_{analysis_config['skills_file_name']}",
    )
