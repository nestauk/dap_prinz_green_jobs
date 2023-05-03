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
from dap_prinz_green_jobs.getters.ojo import get_ojo_skills_sample
from dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils import (
    format_skills,
    chunks,
)
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

    args = parser.parse_args()

    # load ExtractSkills class with custom config
    logger.info("instantiating Extract Skills class...")
    es = ExtractSkills(args.config_name)

    es.load()
    es.taxonomy_skills.rename(columns={"Unnamed: 0": "id"}, inplace=True)
    # set skill match threshold as 0 to get all skills (although get_top_comparisons sets skills threshold to 0.5 anyway)
    es.taxonomy_info["match_thresholds_dict"]["skill_match_thresh"] = 0

    # load job sample
    logger.info("loading ojo skills sample...")
    skills = get_ojo_skills_sample()
    if not args.production:
        skills = skills.head(batch_size)

    skills_formatted_df = (
        skills.groupby("id")
        .skill_label.apply(list)
        .reset_index()
        .assign(skills_formatted=lambda x: x.skill_label.apply(format_skills))
    )

    skills_formatted_list = skills_formatted_df.skills_formatted.to_list()

    job_id_to_formatted_raw_skills = dict(
        zip(skills_formatted_df.id.to_list(), skills_formatted_list)
    )

    logger.info("extracting green skills...")

    ojo_sample_all_green_skills = {}
    for batch_job_id_to_raw_skills in chunks(
        job_id_to_formatted_raw_skills, batch_size
    ):
        job_skills, skill_hashes = es.skill_mapper.preprocess_job_skills(
            {"predictions": batch_job_id_to_raw_skills}
        )
        # to get the output with the top ten closest skills
        mapped_skills = es.skill_mapper.map_skills(
            es.taxonomy_skills,
            skill_hashes,
            es.taxonomy_info.get("num_hier_levels"),
            es.taxonomy_info.get("skill_type_dict"),
        )
        matched_skills = []
        for job_id, skill_info in job_skills.items():
            job_skill_hashes = skill_info["skill_hashes"]
            matched_skills.append(
                [sk for sk in mapped_skills if sk["ojo_skill_id"] in job_skill_hashes]
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
