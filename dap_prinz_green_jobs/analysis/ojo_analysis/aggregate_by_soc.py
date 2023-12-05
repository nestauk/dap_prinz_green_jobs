from dap_prinz_green_jobs.analysis.ojo_analysis.process_ojo_green_measures import *
from dap_prinz_green_jobs.getters.data_getters import save_to_s3
from dap_prinz_green_jobs import BUCKET_NAME

from datetime import datetime

if __name__ == "__main__":
    # date stamps as defined in https://github.com/nestauk/dap_prinz_green_jobs/issues/75

    production = "True"
    config = "base"
    skills_date_stamp = "20230914"
    occ_date_stamp = "20231002"
    ind_date_stamp = "20231013"

    (
        skill_measures_df,
        occs_measures_df,
        inds_measures_df,
        soc_name_dict,
    ) = load_ojo_green_measures(
        production, config, skills_date_stamp, occ_date_stamp, ind_date_stamp
    )

    ### 1. Merge and clean data so green measures are in a df
    # Clean up green measures and produce two dataframes:
    # 1. numerical green measures;
    # 2. extracted green skills
    all_green_measures_df = merge_green_measures(
        skill_measures_df, occs_measures_df, inds_measures_df, soc_name_dict
    )
    all_green_measures_df, soc_descriptions_dict = add_additional_metadata(
        all_green_measures_df
    )
    all_green_measures_df = filter_large_occs(
        all_green_measures_df, min_num_job_ads=50, occ_col="SOC_2020_name"
    )

    # These functions are for some of the notebook visualisations
    # green_skills_df = create_green_skills_df(all_green_measures_df, occ_col="SOC_2020_name")
    # all_green_measures_df_occ = create_agg_measures_per_occ(all_green_measures_df, occ_col="SOC_2020_name")

    all_skills_df = create_skill_df(skill_measures_df)

    occ_aggregated_df = create_agg_data(
        all_green_measures_df,
        all_skills_df,
        soc_descriptions_dict,
        agg_col="SOC_2020_EXT",
    )

    occ_aggregated_df = get_overall_greenness(occ_aggregated_df)

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
