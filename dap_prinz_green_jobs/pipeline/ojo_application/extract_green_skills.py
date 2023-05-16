"""
Extract green skills pipeline
---------

A pipeline that takes a sample of job adverts and extacts and maps
skills onto a custom green skills taxonomy.

(with default parameters):

python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_skills.py
"""
from dap_prinz_green_jobs.getters.data_getters import (
    save_to_s3,
)
from dap_prinz_green_jobs import BUCKET_NAME, PROJECT_DIR, logger

from typing import List, Dict
from tqdm import tqdm
from ojd_daps_skills.pipeline.extract_skills.extract_skills import (
    ExtractSkills,
)  # import the module
from dap_prinz_green_jobs.getters.ojo_getters import get_ojo_skills_sample
import dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils as sm

from datetime import datetime as date
import argparse

batch_size = 5000

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--config_name",
        default="extract_green_skills_esco",
        type=str,
        help="name of config file",
    )

    parser.add_argument(
        "--production",
        type=bool,
        default=True,
        help="whether to run in production mode",
    )

    parser.add_argument(
        "--skill_threshold", type=int, default=0.7, help="skill match threshold"
    )

    args = parser.parse_args()

    # load ExtractSkills class with custom config
    logger.info("instantiating Extract Skills class...")
    es = ExtractSkills(args.config_name)

    es.load()
    es.taxonomy_skills.rename(columns={"Unnamed: 0": "id"}, inplace=True)

    # load job sample
    logger.info("loading ojo skills sample...")
    skills = get_ojo_skills_sample()
    if not args.production:
        skills = skills.head(batch_size)

    skills_formatted_df = (
        skills.groupby("id")
        .skill_label.apply(list)
        .reset_index()
        .assign(skills_formatted=lambda x: x.skill_label.apply(sm.format_skills))
    )

    skills_formatted_list = skills_formatted_df.skills_formatted.to_list()

    job_id_to_formatted_raw_skills = dict(
        zip(skills_formatted_df.id.to_list(), skills_formatted_list)
    )

    logger.info("extracting green skills...")

    ojo_sample_all_green_skills = {}
    for batch_job_id_to_raw_skills in sm.chunks(
        job_id_to_formatted_raw_skills, batch_size
    ):
        job_skills, skill_hashes = es.skill_mapper.preprocess_job_skills(
            {"predictions": batch_job_id_to_raw_skills}
        )
        # to get the output with the top skill
        matched_skills = sm.get_green_skill_measures(
            es=es,
            raw_skills=list(batch_job_id_to_raw_skills.values()),
            skill_hashes=skill_hashes,
            job_skills=job_skills,
            skill_threshold=args.skill_threshold,
        )
        ojo_sample_all_green_skills.update(
            dict(zip(batch_job_id_to_raw_skills.keys(), matched_skills))
        )

    # save extracted green skills to s3
    date_stamp = str(date.today().date()).replace("-", "")

    if args.production:
        logger.info("saving extracted skills to s3...")
        save_to_s3(
            BUCKET_NAME,
            ojo_sample_all_green_skills,
            f"outputs/data/green_skills/{date_stamp}_{args.config_name}_all_matched_skills.json",
        )
