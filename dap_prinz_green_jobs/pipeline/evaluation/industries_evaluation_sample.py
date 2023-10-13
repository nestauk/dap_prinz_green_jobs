"""
Create a sample dataset to evaluate the SicMapper

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

industries_measures_path = f"outputs/data/ojo_application/extracted_green_measures/{date}/ojo_sample_industry_green_measures_production_{production}_{config}.json"
industries_evaluation_df_path = f"outputs/data/labelled_job_adverts/evaluation/industries/{date}_ojo_sample_industry_green_measures_production_{production}_{config}_evaluation_dataset.csv"

random_state = 64


if __name__ == "__main__":
    logger.info("loading industries measures data...")

    industries_data = load_s3_data(BUCKET_NAME, industries_measures_path)
    industries_data_df = (
        pd.DataFrame(industries_data).T.reset_index().rename(columns={"index": "id"})
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