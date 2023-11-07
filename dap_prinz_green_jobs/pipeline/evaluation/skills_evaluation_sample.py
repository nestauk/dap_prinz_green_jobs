"""
Create a sample dataset to evaluate the skills extraction and mapping to green ESCO

python dap_prinz_green_jobs/pipeline/evaluation/skills_evaluation_sample.py
"""

import pandas as pd

import random
from typing import Dict

from dap_prinz_green_jobs.getters.data_getters import load_s3_data, save_to_s3
from dap_prinz_green_jobs import BUCKET_NAME


def format_skills_data(green_skills_outputs: Dict[str, dict]) -> pd.DataFrame:
    skill_measures_df = (
        pd.DataFrame.from_dict(green_skills_outputs, orient="index")
        .reset_index()
        .rename(columns={"index": "job_id"})
    )

    ents_explode = (
        skill_measures_df[["job_id", "ENTS"]].explode("ENTS").reset_index(drop=True)
    )
    ents_explode["skill_label"] = ents_explode["ENTS"].apply(
        lambda x: x[0] if x else []
    )
    ents_explode = ents_explode.explode("skill_label").reset_index(drop=True)

    green_ents_explode = (
        skill_measures_df[["job_id", "GREEN_ENTS"]]
        .explode("GREEN_ENTS")
        .reset_index(drop=True)
    )
    green_ents_explode["skill_label"] = green_ents_explode["GREEN_ENTS"].apply(
        lambda x: x[0] if isinstance(x, list) else None
    )
    green_ents_explode["green_prob"] = green_ents_explode["GREEN_ENTS"].apply(
        lambda x: x[1][1] if isinstance(x, list) else None
    )
    green_ents_explode["green_esco_skill"] = green_ents_explode["GREEN_ENTS"].apply(
        lambda x: x[1][2][0] if isinstance(x, list) else None
    )
    green_ents_explode["green_esco_skill_id"] = green_ents_explode["GREEN_ENTS"].apply(
        lambda x: x[1][2][1] if isinstance(x, list) else None
    )
    green_ents_explode["green_esco_skill_prob"] = green_ents_explode[
        "GREEN_ENTS"
    ].apply(lambda x: x[1][2][2] if isinstance(x, list) else None)

    skills_df = pd.concat([ents_explode, green_ents_explode])
    skills_df = skills_df[
        ((skills_df["skill_label"] != "") & (pd.notnull(skills_df["skill_label"])))
    ]

    # Remove the duplicate green skills per job advert
    skills_df.sort_values(by="green_esco_skill", inplace=True)
    skills_df.drop_duplicates(
        subset=["job_id", "skill_label"], keep="first", inplace=True
    )

    return skills_df


if __name__ == "__main__":
    date_stamp = "20230914"
    production = "True"
    config = "base"

    green_skills_outputs = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/ojo_application/extracted_green_measures/{date_stamp}/ojo_sample_skills_green_measures_production_{production}_{config}.json",
    )

    all_esco_mappings = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/green_skill_lists/20230914/full_esco_skill_mappings.json",
    )

    skills_df = format_skills_data(green_skills_outputs)

    # Get the full ESCO skill mappings
    skills_df["all_esco_map"] = skills_df["skill_label"].map(all_esco_mappings)
    skills_df["esco_skill"] = skills_df["all_esco_map"].apply(lambda x: x[0])
    skills_df["esco_skill_id"] = skills_df["all_esco_map"].apply(lambda x: x[1])
    skills_df["esco_skill_prob"] = skills_df["all_esco_map"].apply(lambda x: x[2])

    # Save a sample of ones with green ESCO mappings and ones without
    skills_df["is_green"] = skills_df["green_esco_skill"].apply(
        lambda x: False if pd.isnull(x) else True
    )
    skills_df_sample = skills_df.groupby("is_green").apply(
        lambda x: x.sample(500, random_state=42)
    )
    skills_df_sample.drop(columns=["ENTS", "GREEN_ENTS", "all_esco_map"], inplace=True)

    save_to_s3(
        BUCKET_NAME,
        skills_df_sample,
        "outputs/data/labelled_job_adverts/evaluation/skills/skill_evaluation_sample.csv",
    )
