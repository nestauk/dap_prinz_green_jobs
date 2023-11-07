"""
This one-off script script cleans and generates datasets needed to calculate industry measures.

Companies House:
    - The full companies house data is really big (2.4GB), so this script removes unneccessary columns and uploads to S3.
    - It also deletes rows which dont have any SIC codes.
    - also saves a dictionary of cleaned company names to SIC codes to S3.

Greenhouse gas emissions:
    - clean greenhouse gas emissions data and saves it + as a dictionary to S3.


 To run:

    python dap_prinz_green_jobs/pipeline/green_measures/industries/industries_data_processing.py

NOTE: This script takes a long time to run as it has to collate all the SIC codes for each company name.
"""

import pandas as pd
from tqdm import tqdm

from dap_prinz_green_jobs.getters.data_getters import save_to_s3
from dap_prinz_green_jobs import BUCKET_NAME
from dap_prinz_green_jobs.pipeline.green_measures.industries.sic_mapper.sic_mapper_utils import (
    clean_company_name,
)

if __name__ == "__main__":
    companies_house = pd.read_csv(
        "s3://prinz-green-jobs/inputs/data/industry_data/BasicCompanyDataAsOneFile-2023-05-01.csv",
        usecols=[
            "CompanyName",
            "SICCode.SicText_1",
            "SICCode.SicText_2",
            "SICCode.SicText_3",
            "SICCode.SicText_4",
        ],
    )

    companies_house = companies_house[
        (
            (companies_house["SICCode.SicText_1"] != "None Supplied")
            & (companies_house["SICCode.SicText_1"] != "99999 - Dormant Company")
            & (companies_house["SICCode.SicText_1"] != "74990 - Non-trading company")
        )
    ].reset_index(drop=True)

    # clean name
    companies_house["cleaned_name"] = companies_house["CompanyName"].map(
        clean_company_name
    )
    # For each cleaned name collate all the SIC's given (dont bother saving null columns)
    print("collating SIC codes for each cleaned name...takes a long time....")
    companies_house_cleaned_dict = {}
    for cleaned_name, grouped_ch in tqdm(companies_house.groupby("cleaned_name")):
        grouped_ch.dropna(axis=1, inplace=True)
        companies_house_cleaned_dict[cleaned_name] = grouped_ch[
            grouped_ch.columns.difference(["cleaned_name"])
        ].to_dict(orient="records")

    save_to_s3(
        BUCKET_NAME,
        companies_house,
        "inputs/data/industry_data/BasicCompanyDataAsOneFile-2023-05-01_key_columns_only.csv",
    )

    save_to_s3(
        BUCKET_NAME,
        companies_house_cleaned_dict,
        "outputs/data/green_industries/companies_house_dict.json",
    )
