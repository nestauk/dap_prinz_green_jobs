"""
Minor tweaks to the dataset behind the green jobs explorer
"""

from dap_prinz_green_jobs.getters.data_getters import save_to_s3, load_s3_data
from dap_prinz_green_jobs import BUCKET_NAME, analysis_config

import os
import ast

import pandas as pd

if __name__ == "__main__":
    root_s3_dir = "outputs/data/ojo_application/extracted_green_measures/analysis/"

    # The output of aggregate_by_soc.py

    occ_agg_extra_loaded = load_s3_data(
        BUCKET_NAME,
        os.path.join(
            root_s3_dir,
            f"occupation_aggregated_data_{analysis_config['analysis_files']['agg_soc_date_stamp']}_extra.csv",
        ),
    )

    # Remove any green skills in the top green skills if the number of job advs they feature in is <=10
    occ_agg_extra_loaded["top_5_green_skills"] = occ_agg_extra_loaded[
        "top_5_green_skills"
    ].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) else [])

    new_top_5_green_skills = []
    for green_skills in occ_agg_extra_loaded["top_5_green_skills"].tolist():
        new_top_5_green_skills.append(
            str([g for g in green_skills if g["num_job_ads"] > 10])
        )  # Convert back to a string

    occ_agg_extra_loaded["top_5_green_skills"] = new_top_5_green_skills

    # Make sure all industries are in lowercase (some are all capitals)
    def decap_inds(top_5_sics):
        top_5_sics = ast.literal_eval(top_5_sics)
        new_top_5_sics = []
        for r in top_5_sics:
            if r["sic_name"].isupper():
                r["sic_name"] = r["sic_name"].title()
            new_top_5_sics.append(r)

        # Convert back to string
        return str(new_top_5_sics)

    occ_agg_extra_loaded["top_5_sics"] = occ_agg_extra_loaded["top_5_sics"].apply(
        lambda x: decap_inds(x)
    )

    # Format all the single quotes to be double quotes (needed for the GJE)
    # Due to the pandas saving dicts as single quotes, we need to read it in
    # with the dict columns as strings, and then change them like this.

    for col_name in [
        "top_5_socs",
        "top_5_green_skills",
        "top_5_not_green_skills",
        "top_5_sics",
        "top_5_itl2_quotient",
        "top_5_similar_occs",
    ]:
        occ_agg_extra_loaded[col_name] = occ_agg_extra_loaded[col_name].str.replace(
            "'", '"'
        )

    # Remove betting shop managers as they have a quirk which means many of them
    # have a frequently asked for skill which is misclassified as green
    occ_agg_extra_loaded = occ_agg_extra_loaded[
        occ_agg_extra_loaded["clean_soc_name"] != "Betting shop managers"
    ]
    occ_agg_extra_loaded.reset_index(inplace=True)

    # These columns might be confusing for a user downloading the data, and since we don't use them - delete them
    occ_agg_extra_loaded.drop(
        columns=["occ_topics", "green_topics_lists"], inplace=True
    )

    occ_agg_extra_loaded.sort_values(
        by="average_prop_green_skills", ascending=False, inplace=True
    )
    occ_agg_extra_loaded.reset_index(inplace=True)

    # We will save a new file, since these changes could cause problems
    # when the dataset is used for plotting.
    save_to_s3(
        BUCKET_NAME,
        occ_agg_extra_loaded,
        os.path.join(
            root_s3_dir,
            f"occupation_aggregated_data_{analysis_config['analysis_files']['agg_soc_date_stamp']}_extra_gjeformat.csv",
        ),
    )
