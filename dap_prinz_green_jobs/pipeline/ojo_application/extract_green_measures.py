"""
Extract green measures pipeline
--------------
A pipeline that extracts green measures on a OJO sample
    as defined in dap_prinz_green_jobs/getters/data_getters/ojo.py

It also saves the extracted green measures to s3.

To run the script including with time tracking:
    time python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_measures.py

In production, this script should take about ~15 minutes to run now
"""
from dap_prinz_green_jobs.pipeline.green_measures.green_measures import GreenMeasures
import dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils as sm
from dap_prinz_green_jobs.getters.ojo_getters import (
    get_mixed_ojo_job_title_sample,
    get_mixed_ojo_sample,
)
from dap_prinz_green_jobs import logger
from dap_prinz_green_jobs.getters.data_getters import save_to_s3
from dap_prinz_green_jobs import BUCKET_NAME

import pandas as pd

from argparse import ArgumentParser
from datetime import datetime as date

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--production", action="store_true", default=False)
    parser.add_argument("--config_name", default="base", type=str)

    args = parser.parse_args()
    production = args.production

    # instantiate GreenMeasures class here
    if production:
        gm = GreenMeasures(config_name=args.config_name)
    else:
        gm = GreenMeasures(
            config_name=args.config_name,
            skills_output_folder="outputs/data/green_skill_lists/test",
        )

    # load and reformat relevant data
    logger.info("loading and reformatting datasets...")

    # step 0. load ojo related data - job title, skills and embeddings

    ojo_job_title_raw = get_mixed_ojo_job_title_sample()
    ojo_desc = get_mixed_ojo_sample()

    # We are only processing job adverts which have full texts
    ojo_sample_raw = pd.merge(
        ojo_desc[["id", "description", "job_title_raw"]],
        ojo_job_title_raw[["id", "company_raw"]],
        how="left",
        on="id",
    ).rename(
        columns={
            "job_title_raw": gm.job_title_key,
            "company_raw": gm.company_name_key,
            "id": gm.job_id_key,
            "description": gm.job_text_key,
        }
    )
    ojo_sample_raw[gm.job_id_key] = ojo_sample_raw[gm.job_id_key].astype(
        str
    )  # Just to be consistant
    ojo_sample_raw = list(ojo_sample_raw.T.to_dict().values())

    if not production:
        test_n = 100
        ojo_sample_raw = ojo_sample_raw[:test_n]

    logger.info("extracting green skills...")
    green_skills_outputs = gm.get_skill_measures(job_advert=ojo_sample_raw)

    logger.info("extracting green industries...")
    green_industry_outputs_dict = gm.get_industry_measures(job_advert=ojo_sample_raw)

    logger.info("extracting green occupations...")
    green_occupation_outputs_dict = gm.get_occupation_measures(
        job_advert=ojo_sample_raw
    )

    logger.info("saving green measures to s3...")

    date_stamp = str(date.today().date()).replace("-", "")

    save_to_s3(
        BUCKET_NAME,
        green_skills_outputs,
        f"outputs/data/ojo_application/extracted_green_measures/{date_stamp}/ojo_sample_skills_green_measures_production_{production}_{gm.config_path.split('/')[-1].split('.')[0]}.json",
    )
    save_to_s3(
        BUCKET_NAME,
        green_industry_outputs_dict,
        f"outputs/data/ojo_application/extracted_green_measures/{date_stamp}/ojo_sample_industry_green_measures_production_{production}_{gm.config_path.split('/')[-1].split('.')[0]}.json",
    )
    save_to_s3(
        BUCKET_NAME,
        green_occupation_outputs_dict,
        f"outputs/data/ojo_application/extracted_green_measures/{date_stamp}/ojo_sample_occupation_green_measures_production_{production}_{gm.config_path.split('/')[-1].split('.')[0]}.json",
    )
