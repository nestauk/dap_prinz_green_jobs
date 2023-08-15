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
from dap_prinz_green_jobs import BUCKET_NAME, logger

from dap_prinz_green_jobs.getters.ojo_getters import (
    get_ojo_skills_sample,
    get_extracted_skill_embeddings,
)
from dap_prinz_green_jobs.getters.skill_getters import (
    get_green_skills_taxonomy,
    get_green_skills_taxonomy_embeddings,
)
import dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils as sm

from datetime import datetime as date
import argparse
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity


def find_green_skills(ojo_skills_raw):
    # step 0.1 load embeddings (extracted skills and green skills taxonomy) and green skills taxonomy df
    logger.info("loading embeddings...")
    extracted_skill_embeddings = get_extracted_skill_embeddings()
    extracted_skill_embeddings_skills = list(extracted_skill_embeddings.keys())

    green_taxonomy_embeddings = get_green_skills_taxonomy_embeddings()
    green_taxonomy_embeddings_skills = list(green_taxonomy_embeddings.keys())

    green_skills_taxonomy = get_green_skills_taxonomy()

    if not production:
        test_n = 100
        extracted_skill_embeddings_skills = extracted_skill_embeddings_skills[:test_n]
        extracted_skill_embeddings = {
            i: j
            for i, j in extracted_skill_embeddings.items()
            if i in extracted_skill_embeddings_skills
        }

    logger.info("extracting green skills...")
    similarities = cosine_similarity(
        np.array(list(extracted_skill_embeddings.values())),
        np.array(list(green_taxonomy_embeddings.values())),
    )
    # Top matches for skill chunk
    top_green_skills = sm.get_green_skill_matches(
        extracted_skill_list=extracted_skill_embeddings_skills,
        similarities=similarities,
        green_skills_taxonomy=green_skills_taxonomy,
        skill_threshold=skill_threshold,
    )

    all_extracted_green_skills_dict = {sk[0]: (sk[0], sk[1]) for sk in top_green_skills}

    green_skill_outputs = (
        ojo_skills_raw.assign(
            green_skills=lambda x: x.skill_label.map(all_extracted_green_skills_dict)
        )
        .groupby("id")
        .green_skills.apply(list)
    )

    green_outputs = {
        "SKILL MEASURES": [
            {"job_id": job_id, "skills": skills}
            for job_id, skills in zip(
                list(green_skill_outputs.index), list(green_skill_outputs.values)
            )
        ]
    }

    return green_outputs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

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
    production = args.production
    skill_threshold = args.skill_threshold

    # load job sample
    logger.info("loading ojo skills sample...")
    ojo_skills_raw = (
        get_ojo_skills_sample()
        # drop nas in skill_label
        .dropna(subset=["skill_label"])
    )

    green_outputs = find_green_skills(ojo_skills_raw)

    # save extracted green skills to s3
    date_stamp = str(date.today().date()).replace("-", "")

    if args.production:
        logger.info("saving extracted skills to s3...")
        save_to_s3(
            BUCKET_NAME,
            green_outputs,
            f"outputs/data/green_skills/{date_stamp}_all_matched_skills.json",
        )
