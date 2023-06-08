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
    get_ojo_job_title_sample,
    #    get_extracted_skill_embeddings,
    #    get_ojo_skills_sample,
    get_mapped_green_skills,
)

# from dap_prinz_green_jobs.getters.skill_getters import (
#     get_green_skills_taxonomy,
#     get_green_skills_taxonomy_embeddings,

# )

from dap_prinz_green_jobs import logger
from dap_prinz_green_jobs.getters.data_getters import save_to_s3
from dap_prinz_green_jobs import BUCKET_NAME

import pandas as pd
from argparse import ArgumentParser
from tqdm import tqdm
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

# instantiate GreenMeasures class here
gm = GreenMeasures(config_name="base")

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--production", type=bool, default=True)

    args = parser.parse_args()

    production = args.production

    # load and reformat relevant data
    logger.info("loading and reformattting datasets...")

    # step 0. load ojo related data - job title, skills and embeddings

    ojo_job_title_raw = get_ojo_job_title_sample()
    # ojo_skills_raw = (
    #     get_ojo_skills_sample()
    #     # drop nas in skill_label
    #     .dropna(subset=["skill_label"])
    # )
    # # step 0.1 load embeddings (extracted skills and green skills taxonomy) and green skills taxonomy df
    # extracted_skill_embeddings = get_extracted_skill_embeddings()
    # extracted_skill_embeddings_skills = list(extracted_skill_embeddings.keys())

    # green_taxonomy_embeddings = get_green_skills_taxonomy_embeddings()
    # green_taxonomy_embeddings_skills = list(green_taxonomy_embeddings.keys())

    # green_skills_taxonomy = get_green_skills_taxonomy()

    # # reformat it to be a list of dictionaries for GreenMeasures
    ojo_sample_raw = ojo_job_title_raw[["id", "job_title_raw", "company_raw"]].rename(
        columns={
            "job_title_raw": gm.job_title_key,
            "company_raw": gm.company_name_key,
        }
    )
    # we're not using descriptions so lets not load them to save time
    ojo_sample_raw = list(ojo_sample_raw.T.to_dict().values())

    # lets load the already mapped green skills from extract_green_skills.py instead of mapping them again
    green_skills = get_mapped_green_skills()["SKILL MEASURES"]

    if not production:
        test_n = 100
        # extracted_skill_embeddings_skills = extracted_skill_embeddings_skills[:test_n]
        # extracted_skill_embeddings = {
        #     i: j
        #     for i, j in extracted_skill_embeddings.items()
        #     if i in extracted_skill_embeddings_skills
        # }
        ojo_sample_raw = ojo_sample_raw[:test_n]
        green_skills = green_skills[:test_n]

    # logger.info("extracting green skills...")
    # similarities = cosine_similarity(
    #     np.array(list(extracted_skill_embeddings.values())),
    #     np.array(list(green_taxonomy_embeddings.values())),
    # )
    # # Top matches for skill chunk
    # top_green_skills = sm.get_green_skill_matches(
    #     extracted_skill_list=extracted_skill_embeddings_skills,
    #     similarities=similarities,
    #     green_skills_taxonomy=green_skills_taxonomy,
    #     skill_threshold=gm.skill_threshold,
    # )

    # all_extracted_green_skills_dict = {sk[0]: (sk[0], sk[1]) for sk in top_green_skills}

    # green_skill_outputs = (
    #     ojo_skills_raw.assign(
    #         green_skills=lambda x: x.skill_label.map(all_extracted_green_skills_dict)
    #     )
    #     .groupby("id")
    #     .green_skills.apply(list)
    # )

    logger.info("extracting green industries...")
    green_industry_outputs = gm.get_industry_measures(job_advert=ojo_sample_raw)
    logger.info("extracting green occupations...")
    green_occupation_outputs = gm.get_occupation_measures(job_advert=ojo_sample_raw)

    # create dictionary with all green measures - where each measure type includes jobs ids
    # to accomodate for the fact that not all job adverts had skills extracted
    logger.info("collating green measures...")
    job_ids = [i["id"] for i in ojo_sample_raw]
    for i, occ in enumerate(green_occupation_outputs):
        occ["job_id"] = job_ids[i]

    green_outputs = {
        "SKILL MEASURES": green_skills,
        "INDUSTRY MEASURES": [
            {"job_id": job_id, "industry_ghg_emissions": emissions}
            for job_id, emissions in zip(
                job_ids, green_industry_outputs["INDUSTRY GHG EMISSIONS"]
            )
        ],
        "OCCUPATION MEASURES": green_occupation_outputs,
    }

    logger.info("saving green measures to s3...")

    save_to_s3(
        BUCKET_NAME,
        green_outputs,
        f"outputs/data/ojo_application/extracted_green_measures/ojo_sample_green_measures_production_{production}_{gm.config_path.split('/')[-1].split('.')[0]}.json",
    )
