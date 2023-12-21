"""
Small script to merge the outputs of aggregate_by_soc.py with occupation_similarity.py
"""

import dap_prinz_green_jobs.analysis.ojo_analysis.process_ojo_green_measures as pg
from dap_prinz_green_jobs.getters.data_getters import save_to_s3, load_s3_data
from dap_prinz_green_jobs import BUCKET_NAME, PROJECT_DIR, logger, analysis_config

import os

if __name__ == "__main__":
    root_s3_dir = "outputs/data/ojo_application/extracted_green_measures/analysis/"

    occ_agg_path = os.path.join(
        root_s3_dir,
        f"occupation_aggregated_data_{analysis_config['analysis_files']['agg_soc_date_stamp']}.csv",
    )

    occ_agg = load_s3_data(
        BUCKET_NAME,
        occ_agg_path,
    )

    occ_most_similar_path = os.path.join(
        root_s3_dir,
        f"occupation_similarity/{analysis_config['analysis_files']['occ_most_similar_date']}/occ_most_similar.json",
    )
    occ_most_similar = load_s3_data(
        BUCKET_NAME,
        occ_most_similar_path,
    )

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

    occ_agg_extra = occ_agg.copy()
    occ_agg_extra["top_5_similar_occs"] = occ_agg_extra["clean_soc_name"].map(
        most_sim_occs_by_soc_id
    )

    save_to_s3(BUCKET_NAME, occ_agg_extra, occ_agg_path.replace(".csv", "_extra.csv"))
