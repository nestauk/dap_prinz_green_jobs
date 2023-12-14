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
)

from datetime import datetime
import yaml
import os

if __name__ == "__main__":
    job_id_col = analysis_config["job_id_col"]
    data_type = analysis_config["data_type"]
    skill_match_thresh = analysis_config["skill_match_thresh"]

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
    else:
        print("set data_type in config to mixed or large")

    all_green_measures_df, soc_descriptions_dict = pg.add_additional_metadata(
        all_green_measures_df, salary_information, locations_information
    )
    all_green_measures_df = pg.filter_large_occs(
        all_green_measures_df, min_num_job_ads=50, occ_col="SOC_2020_name"
    )

    all_skills_df = pg.create_skill_df(
        skill_measures_df, skill_match_thresh=skill_match_thresh
    )

    occ_aggregated_df = pg.create_agg_data(
        all_green_measures_df,
        all_skills_df,
        soc_descriptions_dict,
        agg_col="SOC_2020_EXT",
    )

    occ_aggregated_df = pg.get_overall_greenness(occ_aggregated_df)

    # Save

    today = datetime.now().strftime("%Y%m%d")

    save_to_s3(
        BUCKET_NAME,
        occ_aggregated_df,
        f"outputs/data/ojo_application/extracted_green_measures/analysis/occupation_aggregated_data_{today}.csv",
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
            f"outputs/data/ojo_application/extracted_green_measures/analysis/prop_green_skills_per_occ_{itl_col}_{today}.csv",
        )
