"""
Create a dataset of green industries for the sample of OJO data
"""

import pandas as pd

from dap_prinz_green_jobs.pipeline.green_measures.industries.industry_measures_utils import *
from dap_prinz_green_jobs.getters.ojo import get_ojo_job_title_sample
from dap_prinz_green_jobs.getters.industry_getters import load_industry_ghg
from dap_prinz_green_jobs.getters.data_getters import save_to_s3
from dap_prinz_green_jobs import BUCKET_NAME, logger, config

if __name__ == "__main__":
    ojo_title_data = get_ojo_job_title_sample()

    ojo_title_data["cleaned_name"] = ojo_title_data["company_raw"].map(
        clean_company_name
    )

    ojo_company_names_cleaned = set(ojo_title_data["cleaned_name"].tolist())

    print("load and process companies house")
    companies_house_cleaned_in_ojo_dict = process_companies_house(
        ojo_company_names_cleaned
    )

    # save_to_s3(BUCKET_NAME, companies_house_cleaned_in_ojo_dict, 'outputs/data/ojo_application/industries/companies_house_sample_processed.json')

    ojo_title_data["sic_name"] = ojo_title_data["cleaned_name"].apply(
        lambda x: get_ch_sic(companies_house_cleaned_in_ojo_dict, x)
    )

    ojo_title_data["sic"] = ojo_title_data["sic_name"].apply(clean_sic)

    emissions_data = load_industry_ghg()

    ghg_emissions_dict = dict(
        zip(emissions_data["Unnamed: 0"].tolist(), emissions_data[2020].tolist())
    )

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
