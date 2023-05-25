"""
Extract green measures pipeline
--------------
A pipeline that extracts green measures on a OJO sample
    as defined in dap_prinz_green_jobs/getters/data_getters/ojo.py

It also saves the extracted green measures to s3.

python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_measures.py
"""

from dap_prinz_green_jobs.pipeline.green_measures.green_measures import GreenMeasures
import dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils as sm

from dap_prinz_green_jobs.getters.ojo_getters import (
    get_ojo_skills_sample,
    get_ojo_sample,
    get_ojo_job_title_sample,
)
from dap_prinz_green_jobs import logger
from dap_prinz_green_jobs.getters.data_getters import save_to_s3, load_s3_data
from dap_prinz_green_jobs import BUCKET_NAME

import pandas as pd
from toolz import partition_all
from argparse import ArgumentParser
from uuid import uuid4
from tqdm import tqdm

# instantiate GreenMeasures class here
gm = GreenMeasures(config_name="base")

# load and reformat relevant data
logger.info("loading and reformattting datasets...")
ojo_skills_raw = (
    get_ojo_skills_sample()
    # drop in skill_label
    .dropna(subset=["skill_label"])
)

ojo_skills_list = ojo_skills_raw.skill_label.unique().tolist()

ojo_job_title_raw, ojo_sample_raw = get_ojo_job_title_sample(), get_ojo_sample()
ojo_sample_raw_title = pd.merge(ojo_sample_raw, ojo_job_title_raw, on="id")

# reformat it to be a list of dictionaries for GreenMeasures
ojo_sample = list(
    (
        ojo_sample_raw_title[["id", "job_title_raw_x", "company_raw", "description"]]
        .rename(
            columns={
                "job_title_raw_x": gm.job_title_key,
                "company_raw": gm.company_name_key,
                "description": gm.job_text_key,
            }
        )
        .T.to_dict()
        .values()
    )
)

extracted_skill_embeddings = load_s3_data(
    BUCKET_NAME, "green_measures/skills/skill_embeddings.json"
)

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--production", type=bool, default=False)
    parser.add_argument("--batch_size", type=int, default=10000)

    args = parser.parse_args()

    production = args.production
    batch_size = args.batch_size

    if not production:
        test_n = 100
        ojo_skills_list = ojo_skills_list[:test_n]
        ojo_skills_raw = ojo_skills_raw[:test_n]
        ojo_sample = ojo_sample[:test_n]
        batch_size = test_n

    logger.info("extracting green skills...")
    all_extracted_green_skills = []
    skill_chunks = list(partition_all(batch_size, ojo_skills_list))
    for skill_chunk in tqdm(skill_chunks):
        formatted_raw_skills = gm.es.format_skills(skill_chunk)
        # get job skills and skill hashes
        job_skills, skill_hashes = gm.es.skill_mapper.preprocess_job_skills(
            {
                "predictions": dict(
                    zip(
                        [
                            str(uuid4()).replace("-", "")
                            for _ in range(len(formatted_raw_skills))
                        ],
                        [skill for skill in formatted_raw_skills],
                    )
                )
            }
        )
        formatted_raw_skills_green = sm.get_green_skill_measures(
            es=gm.es,
            raw_skills=formatted_raw_skills,
            skill_hashes=skill_hashes,
            job_skills=job_skills,
            skill_threshold=gm.skill_threshold,
        )
        all_extracted_green_skills.extend(formatted_raw_skills_green[0]["SKILL"])

    all_extracted_green_skills_dict = {
        sk[0]: (sk[0], sk[1]) for sk in all_extracted_green_skills
    }

    green_skill_outputs = (
        ojo_skills_raw.assign(
            green_skills=lambda x: x.skill_label.map(all_extracted_green_skills_dict)
        )
        .groupby("id")
        .green_skills.apply(list)
        .to_list()
    )

    logger.info("extracting green industries...")
    green_industry_outputs = gm.get_industry_measures(job_advert=ojo_sample)
    logger.info("extracting green occupations...")
    green_occupation_outputs = gm.get_occupation_measures(job_advert=ojo_sample)

    # create dictionary with all green measures
    green_outputs = {
        "SKILL MEASURES": green_skill_outputs,
        "INDUSTRY MEASURES": green_industry_outputs,
        "OCCUPATION MEASURES": green_occupation_outputs,
        "job_ids": [job["id"] for job in ojo_sample],
    }

    logger.info("saving green measures to s3...")
    save_to_s3(
        BUCKET_NAME,
        green_outputs,
        f"outputs/data/ojo_application/extracted_green_measures/ojo_sample_green_measures_production_{production}_{gm.config_name}.json",
    )
