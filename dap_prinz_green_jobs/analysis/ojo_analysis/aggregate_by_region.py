"""
Create a dataset of green measures aggregated by region.

Run with:
python dap_prinz_green_jobs/analysis/ojo_analysis/aggregate_by_region.py
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

    all_green_measures_df = pg.merge_green_measures(
        skill_measures_df, occs_measures_df, inds_measures_df, soc_name_dict
    )

    # Add some additional data
    if data_type == "mixed":
        salary_information = get_mixed_ojo_salaries_sample()
        locations_information = get_mixed_ojo_location_sample()
    elif data_type == "large":
        salary_information = get_large_ojo_location_sample()
        locations_information = get_large_ojo_location_sample()
    else:
        print("set data_type in config to mixed or large")

    all_green_measures_df = pg.add_salaries(
        salary_information, all_green_measures_df, job_id_col=job_id_col
    )
    all_green_measures_df = pg.add_locations(
        locations_information, all_green_measures_df, job_id_col=job_id_col
    )
    all_green_measures_df = pg.add_sic_info(all_green_measures_df)

    all_skills_df = pg.create_skill_df(
        skill_measures_df, skill_match_thresh=skill_match_thresh
    )

    agg_itl_by = "itl_3_code"  # itl_2_code or itl_3_code

    itl_aggregated_data = pg.create_agg_data(
        all_green_measures_df,
        all_skills_df,
        soc_descriptions_dict=None,
        agg_col=agg_itl_by,
        job_id_col="job_id",
    )

    # Clean up for tooltips in plotting
    itl_aggregated_data["top_3_sics_names"] = itl_aggregated_data["top_5_sics"].apply(
        lambda x: ", ".join([s["sic_name"] for s in x[0:3]])
    )
    itl_aggregated_data["top_3_green_skills_names"] = itl_aggregated_data[
        "top_5_green_skills"
    ].apply(lambda x: ", ".join([s["skill_name"] for s in x[0:3]]))
    itl_aggregated_data["top_3_socs_names"] = itl_aggregated_data["top_5_socs"].apply(
        lambda x: ", ".join([s["soc_name"] for s in x[0:3]])
    )

    # Get the relative greenness measures across regions
    itl_aggregated_data["occ_greenness"] = itl_aggregated_data[
        "average_occ_green_timeshare"
    ].apply(
        lambda x: pg.categorical_assign(
            x, itl_aggregated_data["average_occ_green_timeshare"]
        )
    )
    itl_aggregated_data["ind_greenness"] = itl_aggregated_data[
        "average_ind_perunit_ghg"
    ].apply(
        lambda x: pg.categorical_assign(
            x, itl_aggregated_data["average_ind_perunit_ghg"], rev=True
        )
    )

    itl_aggregated_data["skills_greenness"] = itl_aggregated_data[
        "average_prop_green_skills"
    ].apply(
        lambda x: pg.categorical_assign(
            x, itl_aggregated_data["average_prop_green_skills"]
        )
    )

    itl_aggregated_data["greenness_score"] = itl_aggregated_data.apply(
        lambda x: pg.get_one_score(
            x["occ_greenness"], x["ind_greenness"], x["skills_greenness"]
        ),
        axis=1,
    )

    today = datetime.now().strftime("%Y%m%d")
    save_to_s3(
        BUCKET_NAME,
        itl_aggregated_data,
        f"outputs/data/ojo_application/extracted_green_measures/analysis/{agg_itl_by}_aggregated_data_{today}.csv",
    )
