"""
Create a dataset of green industries for the sample of OJO data
"""

import pandas as pd

from dap_prinz_green_jobs.pipeline.green_measures.industries.industry_measures_utils import (
    get_green_industry_measure,
)
from dap_prinz_green_jobs.getters.ojo import get_ojo_job_title_sample

if __name__ == "__main__":
    ojo_job_title_data = get_ojo_job_title_sample()

    # WHILST OJO DATA IS UPDATED
    ojo_job_title_data["company_raw"] = ["Company Inc"] * len(ojo_job_title_data)

    ojo_job_title_data["Green/Non-green"] = ojo_job_title_data["company_raw"].apply(
        lambda x: get_green_industry_measure(x) if x else None
    )

    print(ojo_job_title_data["Green/Non-green"].value_counts(dropna=False))
