"""
Create a dataset of green measures aggregated by occupation.

Run with:
python dap_prinz_green_jobs/analysis/ojo_analysis/aggregate_by_soc.py
"""

import dap_prinz_green_jobs.analysis.ojo_analysis.process_ojo_green_measures as pg
from dap_prinz_green_jobs.getters.data_getters import save_to_s3
from dap_prinz_green_jobs import BUCKET_NAME, PROJECT_DIR, analysis_config
from dap_prinz_green_jobs.getters.ojo_getters import (
    get_mixed_ojo_location_sample,
    get_mixed_ojo_salaries_sample,
    get_large_ojo_location_sample,
    get_large_ojo_salaries_sample,
    get_all_ojo_location_sample,
    get_all_ojo_salaries_sample,
)
from dap_prinz_green_jobs.analysis.ojo_analysis.occupation_similarity import (
    run_occupational_similarity,
)

from datetime import datetime
import os

if __name__ == "__main__":
    root_s3_dir = "outputs/data/ojo_application/extracted_green_measures/analysis/"

    job_id_col = analysis_config["job_id_col"]
    data_type = analysis_config["data_type"]
    skill_match_thresh = analysis_config["skill_match_thresh"]
    min_num_job_ads = analysis_config["min_num_job_ads"]

    (
        skill_measures_df,
        occs_measures_df,
        inds_measures_df,
        soc_name_dict,
    ) = pg.load_ojo_green_measures(analysis_config)

    ### 1. Merge and clean data so green measures are in a df
    # Clean up green measures and produce two dataframes:
    # 1. numerical green measures;
    # 2. extracted green skills
    all_green_measures_df = pg.merge_green_measures(
        skill_measures_df, occs_measures_df, inds_measures_df, soc_name_dict
    )

    if data_type == "mixed":
        salary_information = get_mixed_ojo_salaries_sample()
        locations_information = get_mixed_ojo_location_sample()
    elif data_type == "large":
        salary_information = get_large_ojo_salaries_sample()
        locations_information = get_large_ojo_location_sample()
    elif data_type == "all":
        salary_information = get_all_ojo_salaries_sample()
        locations_information = get_all_ojo_location_sample()
    else:
        print("set data_type in config to mixed, large or all")

    all_green_measures_df, soc_descriptions_dict = pg.add_additional_metadata(
        all_green_measures_df, salary_information, locations_information
    )
    all_green_measures_df = pg.filter_large_occs(
        all_green_measures_df, min_num_job_ads=min_num_job_ads, occ_col="SOC_2020_name"
    )

    all_skills_df = pg.load_skills_df(analysis_config)

    green_skill_id_2_name, full_skill_id_2_name = pg.read_process_taxonomies()

    occ_aggregated_df = pg.create_agg_data(
        all_green_measures_df,
        all_skills_df,
        green_skill_id_2_name=green_skill_id_2_name,
        full_skill_id_2_name=full_skill_id_2_name,
        soc_descriptions_dict=soc_descriptions_dict,
        agg_col="SOC_2020_EXT",
    )

    occ_aggregated_df = pg.get_overall_greenness(occ_aggregated_df)
    occ_aggregated_df_filter = occ_aggregated_df[occ_aggregated_df["num_job_ads"] > 50]

    # Save

    today = datetime.now().strftime("%Y%m%d")

    save_to_s3(
        BUCKET_NAME,
        occ_aggregated_df,
        os.path.join(root_s3_dir, f"occupation_aggregated_data_{today}_all.csv"),
    )

    save_to_s3(
        BUCKET_NAME,
        occ_aggregated_df_filter,
        os.path.join(root_s3_dir, f"occupation_aggregated_data_{today}.csv"),
    )

    # Group by occupation and ITL
    for itl_col in ["itl_3_code", "itl_2_code", "itl_1_code"]:
        df = (
            all_green_measures_df.groupby(["SOC_2020_name", itl_col])
            .aggregate(
                {
                    "PROP_GREEN": ["mean"],
                    "job_id": ["count"],
                }
            )
            .reset_index()
        )
        df.columns = df.columns.levels[0]
        df.columns = ["SOC_2020_name", itl_col, "mean_PROP_GREEN", "num_job_ads"]
        save_to_s3(
            BUCKET_NAME,
            df,
            os.path.join(
                root_s3_dir, f"prop_green_skills_per_occ_{itl_col}_{today}.csv"
            ),
        )

    print("Run the occupational similarities")

    (
        esco_id_2_name,
        occ_skills_info,
        green_esco_id,
        occ_most_similar,
    ) = run_occupational_similarity(
        all_skills_df,
        occs_measures_df,
        soc_name_dict,
        occ_aggregated_df,
        green_skill_id_2_name,
        full_skill_id_2_name,
    )

    occ_sim_folder = os.path.join(root_s3_dir, f"occupation_similarity/{today}")

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
    save_to_s3(
        BUCKET_NAME,
        occ_most_similar,
        f"{occ_sim_folder}/occ_most_similar.json",
    )

    # Include occupational similarities in aggregated data output

    most_sim_occs_by_soc_id = {}
    for soc_name, sim_occs_list in occ_most_similar.items():
        most_sim_occs_by_soc_id[pg.clean_soc_name(soc_name)] = [
            {
                "SOC_2020_EXT_name": pg.clean_soc_name(occ["SOC_2020_EXT_name"]),
                "occ_greenness": occ["occ_greenness"],
                "ind_greenness": occ["ind_greenness"],
                "skills_greenness": occ["skills_greenness"],
                "greenness_score": occ["greenness_score"],
            }
            for occ in sim_occs_list[0:5]
        ]

    occ_agg_extra = occ_aggregated_df_filter.copy()
    occ_agg_extra["top_5_similar_occs"] = occ_agg_extra["clean_soc_name"].map(
        most_sim_occs_by_soc_id
    )

    save_to_s3(
        BUCKET_NAME,
        occ_agg_extra,
        os.path.join(root_s3_dir, f"occupation_aggregated_data_{today}_extra.csv"),
    )
