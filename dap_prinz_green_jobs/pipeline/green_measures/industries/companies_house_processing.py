"""
The full companies house data is really big (2.4GB), so this script removes unneccessary columns and uploads to S3.
It also deletes rows which dont have any SIC codes.
"""

import pandas as pd
import openpyxl

from dap_prinz_green_jobs.getters.data_getters import save_to_s3
from dap_prinz_green_jobs import BUCKET_NAME, logger, config

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
        )
    ]
    companies_house.reset_index(drop=True, inplace=True)

    save_to_s3(
        BUCKET_NAME,
        companies_house,
        "inputs/data/industry_data/BasicCompanyDataAsOneFile-2023-05-01_key_columns_only.csv",
    )
