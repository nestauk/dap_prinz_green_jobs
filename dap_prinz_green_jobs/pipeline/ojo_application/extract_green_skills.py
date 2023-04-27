"""
Extract green skills pipeline
---------

A pipeline that takes a sample of job adverts and extacts and maps
skills onto a custom green skills taxonomy.

(with default parameters):

python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_skills.py
"""
from dap_prinz_green_jobs.getters.data_getters import (
    get_s3_resource,
    save_to_s3,
)
from dap_prinz_green_jobs import BUCKET_NAME, PROJECT_DIR, logger

from typing import List, Dict
from tqdm import tqdm
from itertools import islice
from ojd_daps_skills.pipeline.extract_skills.extract_skills import (
    ExtractSkills,
)  # import the module
from dap_prinz_green_jobs.getters.ojo import get_ojo_sample
from datetime import datetime as date
import argparse

s3 = get_s3_resource()
batch_size = 5000


def format_extracted_green_skills(
    job_ids: List[str],
    skills: List[Dict[str, list]],
    matched_skills: List[Dict[str, list]],
) -> Dict[int, dict]:
    """Format matched skills to flag extracted skills that do not
        otherwise match to the green skills taxonomy.

    Args:
        job_ids (List[str]): List of job advert ids
        skills (List[Dict[str, list]]): List of dictionaries of extracted skills
        matched_skills (List[Dict[str, list]]): List of dictionaries of mapped skills

    Returns:
        Dict[int, dict]: Dictionary of job advert ids and their extracted and mapped skills
    """

    formatted_green_skills_dict = {}
    for i, (skill, green_skill) in enumerate(zip(skills, matched_skills)):
        if green_skill != {}:
            formatted_green_skills_dict[job_ids[i]] = green_skill
        else:
            if skill["SKILL"] != []:
                formatted_green_skills_dict[job_ids[i]] = {
                    "SKILL": [
                        (sk, ("not a green skill", None)) for sk in skill["SKILL"]
                    ]
                }
            else:
                formatted_green_skills_dict[job_ids[i]] = {"SKILL": []}

    return formatted_green_skills_dict


def chunks(data_dict: dict, chunk_size=100):
    """Chunks data dictionary into batches of a specified chunk_size.

    Args:
        data_dict (_type_): dictionary of job adverts
        chunk_size (int, optional): chunk size. Defaults to 100.

    Yields:
        _type_: job advert chunks
    """
    it = iter(data_dict)
    for i in range(0, len(data_dict), chunk_size):
        yield {k: data_dict[k] for k in islice(it, chunk_size)}


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

    # load job sample
    logger.info("loading ojo sample...")
    ojo_data = get_ojo_sample()

    if not args.production:
        ojo_data = ojo_data.head(batch_size)

    job_adverts = dict(zip(ojo_data.job_id.to_list(), ojo_data.text.to_list()))

    logger.info("extracting green skills...")
    ojo_sample_green_skills = {}
    for batch_job_adverts in chunks(job_adverts, batch_size):
        batch_raw_skills = es.get_skills(batch_job_adverts.values())
        mapped_skills = es.map_skills(batch_raw_skills)
        ojo_sample_green_skills.update(
            format_extracted_green_skills(
                list(batch_job_adverts.keys()), batch_raw_skills, mapped_skills
            )
        )

    # save extracted green skills to s3
    date_stamp = str(date.today().date()).replace("-", "")

    if args.production:
        logger.info("saving extracted skills to s3...")
        save_to_s3(
            s3,
            BUCKET_NAME,
            ojo_sample_green_skills,
            f"outputs/data/green_skills/{date_stamp}_{args.config_name}_job_adverts.json",
        )
