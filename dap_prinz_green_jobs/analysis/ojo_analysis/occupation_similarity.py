"""
Get counts and proportions of skills in each occupation.
Use these to create occupation skill similarity measures.
"""

import dap_prinz_green_jobs.analysis.ojo_analysis.process_ojo_green_measures as pg
from dap_prinz_green_jobs.getters.data_getters import save_to_s3, load_s3_data
from dap_prinz_green_jobs import BUCKET_NAME, PROJECT_DIR, logger, analysis_config
from dap_prinz_green_jobs.getters.ojo_getters import (
    get_mixed_ojo_location_sample,
    get_mixed_ojo_salaries_sample,
    get_large_ojo_location_sample,
    get_large_ojo_salaries_sample,
)

from datetime import datetime
import yaml
import os

import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from tqdm import tqdm


def find_skill_similarity(
    occ_skills_info: dict,
    esco_id_2_name: dict,
    green_esco_id: list,
    occ_agg_data: str = "outputs/data/ojo_application/extracted_green_measures/analysis/occupation_aggregated_data_20231220_all.csv",
    top_n: int = 10,
) -> dict:
    """
    Find the most similar occupations based off skills asked for, and
    provide some other information about these occupations in the output
    """
    # Convert dicts to cooccurrence matrix
    skills_props_per_occ = {k: v["skill_props"] for k, v in occ_skills_info.items()}
    skills_props_per_occ_df = pd.DataFrame(skills_props_per_occ)
    skills_props_per_occ_df = skills_props_per_occ_df.T
    skills_props_per_occ_df.fillna(value=0, inplace=True)
    # Map column names back to their ESCO codes
    skills_props_per_occ_df.rename(columns=esco_id_2_name, inplace=True)
    similarities = cosine_similarity(
        np.array(skills_props_per_occ_df),
        np.array(skills_props_per_occ_df),
    )
    # Get extra information aboutt he occupations using the aggregated by occupation dataset
    # and find most similar occupations
    occ_agg = load_s3_data(
        BUCKET_NAME,
        occ_agg_data,
    )
    soc_2_propgreen = dict(
        zip(occ_agg["SOC_2020_EXT"], occ_agg["average_prop_green_skills"])
    )
    soc_2num = dict(zip(occ_agg["SOC_2020_EXT"], occ_agg["num_job_ads"]))
    soc_2_occ_greenness = dict(zip(occ_agg["SOC_2020_EXT"], occ_agg["occ_greenness"]))
    soc_2_ind_greenness = dict(zip(occ_agg["SOC_2020_EXT"], occ_agg["ind_greenness"]))
    soc_2_skills_greenness = dict(
        zip(occ_agg["SOC_2020_EXT"], occ_agg["skills_greenness"])
    )
    soc_2_greenness_score = dict(
        zip(occ_agg["SOC_2020_EXT"], occ_agg["greenness_score"])
    )
    soc_name_2_id = {k: v["SOC_2020_EXT"] for k, v in occ_skills_info.items()}
    green_esco_id = set(green_esco_id)
    occ_most_similar = {}
    for occ_ix, occ_name in enumerate(skills_props_per_occ_df.index):
        top_sims = []
        top_skills = set(list(occ_skills_info[occ_name]["skill_counts"])[0:20])
        for most_sim_arg in np.flip(np.argsort(similarities[occ_ix])):
            if most_sim_arg != occ_ix:
                sim_occ_name = skills_props_per_occ_df.index[most_sim_arg]
                if occ_skills_info[sim_occ_name]["num_skills"] > 10:
                    sim_occ_skills = list(
                        occ_skills_info[sim_occ_name]["skill_counts"]
                    )  # Already sorted
                    common_green_skills = [
                        esco_id_2_name[s] for s in sim_occ_skills if s in green_esco_id
                    ][0:5]
                    popular_overlap = top_skills.intersection(set(sim_occ_skills[0:5]))
                    popular_overlap = [esco_id_2_name[s] for s in popular_overlap]
                    top_sims.append(
                        {
                            "SOC_2020_EXT_name": sim_occ_name,
                            "similarity": round(similarities[occ_ix][most_sim_arg], 2),
                            "number of skills": occ_skills_info[sim_occ_name][
                                "num_skills"
                            ],
                            "average number of skills per job advert": round(
                                occ_skills_info[sim_occ_name]["num_skills"]
                                / occ_skills_info[sim_occ_name]["num_job_ids"],
                                2,
                            ),
                            "popular_overlap": popular_overlap,
                            "common_green_skills": common_green_skills,
                            "av_proportion_green_skills": soc_2_propgreen.get(
                                soc_name_2_id[sim_occ_name]
                            ),  # what we use in the rest of the tool
                            # 'av_proportion_green_skills_2':occ_skills_info[sim_occ_name]['prop_green_skills'], # prop only using job adverts with skills
                            "occ_greenness": soc_2_occ_greenness.get(
                                soc_name_2_id[sim_occ_name]
                            ),
                            "ind_greenness": soc_2_ind_greenness.get(
                                soc_name_2_id[sim_occ_name]
                            ),
                            "skills_greenness": soc_2_skills_greenness.get(
                                soc_name_2_id[sim_occ_name]
                            ),
                            "greenness_score": soc_2_greenness_score.get(
                                soc_name_2_id[sim_occ_name]
                            ),
                        }
                    )
        occ_most_similar[occ_name] = top_sims[0:top_n]
    return occ_most_similar


if __name__ == "__main__":
    # Load and process skills data

    green_skills_outputs = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/ojo_application/extracted_green_measures/{analysis_config['skills_date_stamp']}/ojo_large_sample_skills_green_measures_production_{analysis_config['production']}.csv",
    )

    green_skills_outputs["GREEN_ENTS"] = green_skills_outputs["GREEN_ENTS"].apply(
        pg.safe_literal_eval
    )
    green_skills_outputs["ENTS"] = green_skills_outputs["ENTS"].apply(
        pg.safe_literal_eval
    )

    skill_match_thresh = analysis_config["skill_match_thresh"]
    full_skill_mapping = pg.load_full_skill_mapping(analysis_config)
    all_skills_df = pg.create_skill_df(
        green_skills_outputs, full_skill_mapping, skill_match_thresh=skill_match_thresh
    )

    # Find which job ids correspond to which SOC

    green_occs_outputs = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/ojo_application/extracted_green_measures/{analysis_config['occ_date_stamp']}/ojo_large_sample_occupation_green_measures_production_{analysis_config['production'].lower()}.csv",
    )
    green_occs_outputs = pg.process_soc_columns(green_occs_outputs)

    soc_name_dict = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/ojo_application/extracted_green_measures/{analysis_config['occ_date_stamp']}/soc_name_dict.json",
    )

    id_2_occ = dict(
        zip(green_occs_outputs["job_id"], green_occs_outputs["SOC_2020_EXT"])
    )
    soc_2020_6_dict = soc_name_dict["soc_2020_6"]

    # Add the SOC information for each skill

    all_skills_df["SOC_2020_EXT"] = all_skills_df["job_id"].map(id_2_occ)
    all_skills_df["SOC_2020_EXT_name"] = all_skills_df["SOC_2020_EXT"].map(
        soc_2020_6_dict
    )

    # Remove all null esco skill ids
    all_skills_df_not_null_ids = all_skills_df.dropna(
        subset=["extracted_green_skill_id", "extracted_full_skill_id"], how="all"
    )

    # will take a long time
    # Take the green esco name if its there, otherwise the full esco mapping
    all_skills_df_not_null_ids["esco_id"] = np.where(
        pd.notnull(all_skills_df_not_null_ids["extracted_green_skill_id"]),
        all_skills_df_not_null_ids["extracted_green_skill_id"],
        all_skills_df_not_null_ids["extracted_full_skill_id"],
    )
    all_skills_df_not_null_ids["esco_name"] = np.where(
        pd.notnull(all_skills_df_not_null_ids["extracted_green_skill"]),
        all_skills_df_not_null_ids.apply(
            lambda x: x["green_skill_preferred_name"]
            if pd.notnull(x["green_skill_preferred_name"])
            else x["extracted_green_skill"],
            axis=1,
        ),
        all_skills_df_not_null_ids.apply(
            lambda x: x["full_skill_preferred_name"]
            if pd.notnull(x["full_skill_preferred_name"])
            else x["extracted_full_skill"],
            axis=1,
        ),
    )

    all_skills_df_not_null_ids["green_skill"] = pd.notnull(
        all_skills_df_not_null_ids["extracted_green_skill_id"]
    )

    # Per occupation get the skill counts and proportions
    occ_skills_info = {}
    for occ_name, occ_filt_skills in tqdm(
        all_skills_df_not_null_ids.groupby("SOC_2020_EXT_name")
    ):
        occ_skills_info[occ_name] = {
            "skill_counts": occ_filt_skills["esco_id"].value_counts().to_dict(),
            "skill_props": occ_filt_skills["esco_id"]
            .value_counts(normalize=True)
            .to_dict(),
            "prop_green_skills": len(occ_filt_skills[occ_filt_skills["green_skill"]])
            / len(occ_filt_skills),
            "num_skills": len(occ_filt_skills),
            "SOC_2020_EXT": occ_filt_skills["SOC_2020_EXT"].unique()[0],
            "num_job_ids": occ_filt_skills["job_id"].nunique(),
        }

    esco_id_2_name = dict(
        zip(
            all_skills_df_not_null_ids["esco_id"],
            all_skills_df_not_null_ids["esco_name"],
        )
    )
    green_esco_id = (
        all_skills_df_not_null_ids[all_skills_df_not_null_ids["green_skill"]]["esco_id"]
        .unique()
        .tolist()
    )

    # Save useful information for skill similarities

    today = datetime.now().strftime("%Y%m%d")
    occ_sim_folder = f"outputs/data/ojo_application/extracted_green_measures/analysis/occupation_similarity/{today}"

    # Save everything needed to calculate occupation similarity based off skills
    save_to_s3(
        BUCKET_NAME,
        esco_id_2_name,
        f"{occ_sim_folder}/esco_id_2_name.json",
    )
    save_to_s3(
        BUCKET_NAME,
        occ_skills_info,
        f"{occ_sim_folder}/occ_skills_info.json",
    )
    save_to_s3(
        BUCKET_NAME,
        green_esco_id,
        f"{occ_sim_folder}/green_esco_id.json",
    )

    occ_most_similar = find_skill_similarity(
        occ_skills_info, esco_id_2_name, green_esco_id, top_n=10
    )
    occ_most_similar = {pg.clean_soc_name(k): v for k, v in occ_most_similar.items()}

    save_to_s3(
        BUCKET_NAME,
        occ_most_similar,
        f"{occ_sim_folder}/occ_most_similar.json",
    )
