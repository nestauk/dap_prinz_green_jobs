"""
One script to aggregate all the data by SOC, SIC and region.

Created in the Nov 2024 data update to be more efficient then the code that went before, and more specific to the latest small data format changes made in the skills outputs.

"""

from dap_prinz_green_jobs import BUCKET_NAME, logger, analysis_config
from dap_prinz_green_jobs.getters.data_getters import save_to_s3, load_s3_data
import dap_prinz_green_jobs.analysis.ojo_analysis.process_ojo_green_measures as pg
from dap_prinz_green_jobs.getters.data_getters import load_s3_data, get_s3_data_paths
from dap_prinz_green_jobs.analysis.ojo_analysis.occupation_similarity import (
    run_occupational_similarity,
)
from dap_prinz_green_jobs.analysis.ojo_analysis.gje_formatting import (
    run_gje_formatting,
)

import polars as pl
import pandas as pd

from datetime import datetime
import os


def load_combine_green_measures(job_id_col="job_id"):
    logger.info("Loading skills data")
    green_skills_metrics = pd.read_parquet(
        f"s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/{analysis_config['skills_date_stamp']}/{analysis_config['skill_metrics_name']}",
    )

    green_skills = pd.read_parquet(
        f"s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/{analysis_config['skills_date_stamp']}/{analysis_config['green_skills_exploded_name']}",
    )

    # All the skills (not just green)
    all_skills_data = pd.read_parquet(
        f"s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/{analysis_config['skills_date_stamp']}/{analysis_config['all_skills_exploded_name']}"
    )

    logger.info("Loading occupation data")
    green_occs = pd.read_parquet(
        f"s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/{analysis_config['occ_date_stamp']}/{analysis_config['occ_file_name']}",
    )

    green_occs = pg.process_soc_columns(green_occs)
    # In the version of the SOC data we are using there is a mistake where machine learning engineers were
    # coded to '3433/04' which is 'Yoga teachers'. Luckily its an easy fix because the data wasn't incorrect
    # in the 4-digit category or the SOC 2010, so we can quickly find the ones to change to the correct SOC,
    # and the green measures are correct (since they use SOC 2010).
    green_occs.loc[
        (
            (green_occs["SOC_2020_EXT"] == "3433/04")
            & (green_occs["SOC_2020"] == "2134")
        ),
        "SOC_2020_EXT",
    ] = "2134/99"

    soc_name_dict = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/ojo_application/extracted_green_measures/{analysis_config['occ_date_stamp']}/soc_name_dict.json",
    )

    logger.info("Loading industry data")
    green_inds_outputs = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/ojo_application/extracted_green_measures/{analysis_config['ind_date_stamp']}/{analysis_config['ind_file_name']}",
    )

    green_inds_outputs = pg.process_ind_columns(green_inds_outputs)

    # Merge all 3 green measures into one dataframe where each row is a job advert.

    soc_2020_6_dict = soc_name_dict["soc_2020_6"]
    soc_2020_4_dict = soc_name_dict["soc_2020_4"]

    all_green_measures_df = pd.merge(
        green_skills_metrics, green_occs, how="outer", on=job_id_col
    )
    all_green_measures_df = pd.merge(
        all_green_measures_df, green_inds_outputs, how="outer", on=job_id_col
    )

    # replace float with 0
    # all_green_measures_df = all_green_measures_df.fillna("")

    all_green_measures_df.drop(
        columns=[
            "SOC",
            "name",
            "NUM_ORIG_ENTS",
            "num_all_skills_ojo",
            "prop_green_with_hs",
            "SIC_confidence",
            "SIC_method",
            "company_description",
        ],
        inplace=True,
    )

    all_green_measures_df.rename(
        columns={
            job_id_col: "job_id",
            "count_green_skills_no_hs": "NUM_GREEN_ENTS",
        },
        inplace=True,
    )
    all_green_measures_df["SOC_2020_name"] = all_green_measures_df["SOC_2020"].map(
        soc_2020_4_dict
    )
    all_green_measures_df["SOC_2020_EXT_name"] = all_green_measures_df[
        "SOC_2020_EXT"
    ].map(soc_2020_6_dict)

    # all_green_measures_df.replace("", np.nan, inplace=True)

    logger.info(f"There are {len(all_green_measures_df)} rows in the merged data")
    logger.info(f"There are {all_green_measures_df['job_id'].nunique()} unique job ids")

    salary_information = pd.read_parquet(
        os.path.join(
            analysis_config["deduplicated_data_dir"],
            analysis_config["dedupe_salary_name"],
        )
    )
    locations_information = pd.read_parquet(
        os.path.join(
            analysis_config["deduplicated_data_dir"],
            analysis_config["dedupe_location_name"],
        )
    )

    all_green_measures_df, soc_descriptions_dict = pg.add_additional_metadata(
        all_green_measures_df, salary_information, locations_information
    )

    # Combine green and not green skills

    all_skills_df = pd.concat(
        [all_skills_data, green_skills[["job_id", "extracted_green_skill_id"]]]
    )

    return (
        all_green_measures_df,
        soc_descriptions_dict,
        all_skills_df,
        green_occs,
        soc_name_dict,
    )


if __name__ == "__main__":
    today = datetime.now().strftime("%Y%m%d")
    root_s3_dir = f"s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/analysis/{today}"

    job_id_col = "job_id"

    # --------------------------------------

    logger.info("Import and combine all the measures for all the job adverts")

    (
        all_green_measures_df,
        soc_descriptions_dict,
        all_skills_df,
        green_occs,
        soc_name_dict,
    ) = load_combine_green_measures(job_id_col=job_id_col)

    all_green_measures_df.to_parquet(
        os.path.join(root_s3_dir, "combined_green_measures_and_meta.parquet")
    )

    green_skill_id_2_name, full_skill_id_2_name = pg.read_process_taxonomies()

    # --------------------------------------

    logger.info("SOC aggregation")

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

    occ_aggregated_df.to_csv(
        os.path.join(root_s3_dir, f"occupation_aggregated_data_{today}_all.csv"),
        index=False,
    )

    occ_aggregated_df_filter.to_csv(
        os.path.join(root_s3_dir, f"occupation_aggregated_data_{today}.csv"),
        index=False,
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
        df.to_csv(
            os.path.join(
                root_s3_dir, f"prop_green_skills_per_occ_{itl_col}_{today}.csv"
            ),
            index=False,
        )

    # Find occupational similarity and save

    (
        esco_id_2_name,
        occ_skills_info,
        green_esco_id,
        occ_most_similar,
    ) = run_occupational_similarity(
        all_skills_df,
        green_occs,
        soc_name_dict,
        occ_aggregated_df,
        green_skill_id_2_name,
        full_skill_id_2_name,
    )

    # Save everything needed to calculate occupation similarity based off skills

    save_to_s3(
        BUCKET_NAME,
        esco_id_2_name,
        os.path.join(
            root_s3_dir.split("s3://prinz-green-jobs/")[1],
            "occupation_similarity/esco_id_2_name.json",
        ),
    )
    save_to_s3(
        BUCKET_NAME,
        occ_skills_info,
        os.path.join(
            root_s3_dir.split("s3://prinz-green-jobs/")[1],
            "occupation_similarity/occ_skills_info.json",
        ),
    )
    save_to_s3(
        BUCKET_NAME,
        green_esco_id,
        os.path.join(
            root_s3_dir.split("s3://prinz-green-jobs/")[1],
            "occupation_similarity/green_esco_id.json",
        ),
    )
    save_to_s3(
        BUCKET_NAME,
        occ_most_similar,
        os.path.join(
            root_s3_dir.split("s3://prinz-green-jobs/")[1],
            "occupation_similarity/occ_most_similar.json",
        ),
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

    occ_agg_extra.to_csv(
        os.path.join(root_s3_dir, f"occupation_aggregated_data_{today}_extra.csv"),
        index=False,
    )

    occ_agg_extra["num_job_ads"] = occ_agg_extra["num_job_ads"].astype(int)
    occ_agg_extra_gje = run_gje_formatting(occ_agg_extra, fix_ast=False)

    # We will save a new file, since these changes could cause problems
    # when the dataset is used for plotting.
    occ_agg_extra_gje.to_csv(
        os.path.join(
            root_s3_dir, f"occupation_aggregated_data_{today}_extra_gjeformat.csv"
        ),
        index=False,
    )

    # --------------------------------------

    logger.info("SIC aggregation")

    sic_aggregated_df = pg.create_agg_data(
        all_green_measures_df,
        all_skills_df,
        soc_descriptions_dict=soc_descriptions_dict,
        green_skill_id_2_name=green_skill_id_2_name,
        full_skill_id_2_name=full_skill_id_2_name,
        agg_col="SIC",
    )

    sic_aggregated_df = pg.get_overall_greenness(sic_aggregated_df)

    # Should we remove occupation which had few job ads? We did before but I dont see why
    sic_aggregated_df.to_csv(
        os.path.join(root_s3_dir, f"industry_aggregated_data_{today}.csv"), index=False
    )

    # Group by occupation and ITL
    for itl_col in ["itl_3_code", "itl_2_code", "itl_1_code"]:
        df = (
            all_green_measures_df.groupby(["SIC_name", itl_col])
            .aggregate(
                {
                    "PROP_GREEN": ["mean"],
                    "job_id": ["count"],
                }
            )
            .reset_index()
        )
        df.columns = df.columns.levels[0]
        df.columns = ["SIC_name", itl_col, "mean_PROP_GREEN", "num_job_ads"]
        df.to_csv(
            os.path.join(
                root_s3_dir, f"prop_green_skills_per_sic_{itl_col}_{today}.csv"
            ),
            index=False,
        )

    # --------------------------------------

    logger.info("ITL aggregation")

    for agg_itl_by in ["itl_1_code", "itl_2_code", "itl_3_code"]:
        print(agg_itl_by)
        itl_aggregated_data = pg.create_agg_data(
            all_green_measures_df,
            all_skills_df,
            soc_descriptions_dict=None,
            green_skill_id_2_name=green_skill_id_2_name,
            full_skill_id_2_name=full_skill_id_2_name,
            agg_col=agg_itl_by,
            job_id_col="job_id",
        )
        # Clean up for tooltips in plotting
        itl_aggregated_data["top_3_sics_names"] = itl_aggregated_data[
            "top_5_sics"
        ].apply(lambda x: ", ".join(['"' + s["sic_name"] + '"' for s in x[0:3]]))
        itl_aggregated_data["top_3_green_skills_names"] = itl_aggregated_data[
            "top_5_green_skills"
        ].apply(lambda x: ", ".join(['"' + s["skill_name"] + '"' for s in x[0:3]]))
        itl_aggregated_data["top_3_socs_names"] = itl_aggregated_data[
            "top_5_socs"
        ].apply(
            lambda x: ", ".join(
                ['"' + pg.clean_soc_name(s["soc_name"]) + '"' for s in x[0:3]]
            )
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
        itl_aggregated_data.to_csv(
            os.path.join(root_s3_dir, f"{agg_itl_by}_aggregated_data_{today}.csv"),
            index=False,
        )

    all_itl_aggregated = pd.DataFrame()
    for agg_itl_by in ["itl_1_code", "itl_2_code", "itl_3_code"]:
        df = pd.read_csv(
            os.path.join(root_s3_dir, f"{agg_itl_by}_aggregated_data_{today}.csv")
        )
        df.rename(
            columns={
                agg_itl_by: "itl_code",
                agg_itl_by.replace("code", "name"): "itl_name",
            },
            inplace=True,
        )
        df["itl_level"] = agg_itl_by
        all_itl_aggregated = pd.concat([all_itl_aggregated, df])

    all_itl_aggregated.to_csv(
        os.path.join(root_s3_dir, f"all_itl_aggregated_data_{today}.csv"), index=False
    )
