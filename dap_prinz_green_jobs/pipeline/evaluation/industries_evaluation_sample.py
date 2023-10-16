"""
Create sample datasets to evaluate the SicMapper

    python dap_prinz_green_jobs/pipeline/evaluation/industries_evaluation_sample.py
"""

from dap_prinz_green_jobs import BUCKET_NAME, logger
import pandas as pd

from dap_prinz_green_jobs.getters.data_getters import load_s3_data

from dap_prinz_green_jobs.getters.ojo_getters import (
    get_mixed_ojo_job_title_sample,
    get_mixed_ojo_sample,
)

date = "20231013"
production = True
config = "base"
random_state = 64

industries_measures_path = f"outputs/data/ojo_application/extracted_green_measures/{date}/ojo_sample_industry_green_measures_production_{production}_{config}.json"

use_companies_house = False
companies_house_path = f"outputs/data/ojo_application/extracted_green_measures/{date}/ojo_sample_industry_green_measures_production_{production}_{config}_companies_house_{use_companies_house}.json"

industries_evaluation_df_path = f"outputs/data/labelled_job_adverts/evaluation/industries/{date}_ojo_sample_industry_green_measures_production_{production}_{config}_evaluation_dataset.csv"
sic_comparison_path = f"outputs/data/labelled_job_adverts/evaluation/industries/{date}_ojo_sample_industry_green_measures_production_{production}_{config}_sic_comparison_dataset.csv"

if __name__ == "__main__":
    logger.info("loading industries measures data...")

    industries_data = load_s3_data(BUCKET_NAME, industries_measures_path)
    industries_data_df = (
        pd.DataFrame(industries_data).T.reset_index().rename(columns={"index": "id"})
    )

    industries_data_df_no_comp_house = load_s3_data(BUCKET_NAME, companies_house_path)
    industries_data_df_no_comp_house_df = (
        pd.DataFrame(industries_data_df_no_comp_house)
        .T.reset_index()
        .rename(columns={"index": "id"})
    )

    ##add OJO job title and description

    ojo_job_title_raw = get_mixed_ojo_job_title_sample()
    ojo_desc = get_mixed_ojo_sample()

    ojo_merged = pd.merge(ojo_job_title_raw, ojo_desc, on="id", how="left")
    ojo_merged["id"] = ojo_merged["id"].astype(str)

    logger.info("generating evaluation dataset...")
    industries_data_df_ojo = pd.merge(
        industries_data_df, ojo_merged, on="id", how="left"
    )[
        [
            "id",
            "job_title_raw_x",
            "description",
            "SIC",
            "SIC_name",
            "SIC_confidence",
            "SIC_method",
            "company_description",
            "INDUSTRY TOTAL GHG EMISSIONS",
            "INDUSTRY GHG PER UNIT EMISSIONS",
            "INDUSTRY PROP HOURS GREEN TASKS",
            "INDUSTRY PROP WORKERS GREEN TASKS",
            "INDUSTRY PROP WORKERS 20PERC GREEN TASKS",
            "INDUSTRY GHG EMISSIONS PER EMPLOYEE",
            "INDUSTRY CARBON DIOXIDE EMISSIONS PER EMPLOYEE",
        ]
    ].rename(
        columns={"job_title_raw_x": "job_title"}
    )

    industries_measures_sample = (
        industries_data_df_ojo.groupby("SIC_method")
        # take a sample of 200 from each SIC method
        .apply(lambda group: group.sample(n=200, random_state=1)).reset_index(drop=True)
    )

    logger.info("saving evaluation dataset...")
    industries_measures_sample.to_csv(
        f"s3://{BUCKET_NAME}/{industries_evaluation_df_path}", index=False
    )

    # then also create a sample of 100 matches
    # comparing industries measures using
    # companies house data and not using it
    industries_data_comp_house = industries_data_df.query(
        "SIC_method == 'companies house'"
    ).rename(
        columns={"SIC": "SIC_companies_house", "SIC_name": "SIC_name_companies_house"}
    )[
        ["id", "SIC_companies_house", "SIC_name_companies_house"]
    ]

    industries_data_no_comp_house = industries_data_df_no_comp_house_df[
        ["id", "company_description", "SIC", "SIC_name", "SIC_confidence", "SIC_method"]
    ]

    sic_comparison_df = pd.merge(
        industries_data_comp_house, industries_data_no_comp_house, on="id"
    )

    sic_comparison_df_sample = sic_comparison_df.dropna(subset=["SIC"]).sample(
        100, random_state=random_state
    )
    logger.info("saving SIC comparison evaluation dataset...")
    sic_comparison_df_sample.to_csv(
        f"s3://{BUCKET_NAME}/{sic_comparison_path}", index=False
    )
