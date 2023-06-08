"""
Create a dataset of green industries for the sample of OJO data

python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_industries.py
"""

import pandas as pd

from dap_prinz_green_jobs.pipeline.green_measures.industries.industries_measures_utils import *
from dap_prinz_green_jobs.getters.ojo_getters import get_ojo_job_title_sample
from dap_prinz_green_jobs.getters.industry_getters import load_industry_ghg_dict
from dap_prinz_green_jobs.getters.data_getters import save_to_s3
from dap_prinz_green_jobs import BUCKET_NAME, logger, config

if __name__ == "__main__":
    ojo_title_data = get_ojo_job_title_sample()

    ojo_title_data["cleaned_name"] = ojo_title_data["company_raw"].map(
        clean_company_name
    )

    ojo_company_names_cleaned = set(ojo_title_data["cleaned_name"].tolist())

    ojo_title_data["sic_name"] = ojo_title_data["cleaned_name"].apply(
        lambda x: get_ch_sic(cleaned_name=x)
    )

    ojo_title_data["sic"] = ojo_title_data["sic_name"].apply(clean_sic)

    ghg_emissions_dict = load_industry_ghg_dict()

    ojo_title_data["ghg_total_2020"] = ojo_title_data["sic"].apply(
        lambda x: get_ghg_sic(x, ghg_emissions_dict)
    )

    ojo_with_sic = ojo_title_data[ojo_title_data["sic"].notnull()]
    print(
        f"{round(len(ojo_with_sic)*100/len(ojo_title_data), 2)}% of job adverts have a SIC"
    )
    print(
        f"{round(sum(ojo_title_data['ghg_total_2020'].notnull())*100/len(ojo_title_data), 2)}% of job adverts have GHG emissions data"
    )

    ojo_with_sic_not_recruiter = ojo_with_sic[
        ojo_with_sic["type"] != "Recruitment consultancy"
    ]
    print(
        f"{round(len(ojo_with_sic_not_recruiter)*100/len(ojo_with_sic), 2)}% of job adverts with a SIC are not recruitment consultancies"
    )
    print(
        f"{round(sum(ojo_with_sic_not_recruiter['ghg_total_2020'].notnull())*100/len(ojo_with_sic_not_recruiter), 2)}% of job adverts with SIC and not recruiters have GHG emissions data"
    )
