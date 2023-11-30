"""
Run the skills measures for the large sample of job adverts.

python -i dap_prinz_green_jobs/pipeline/ojo_application/flows/ojo_skills_measures.py


"""

from dap_prinz_green_jobs import logger
from dap_prinz_green_jobs.getters.data_getters import (
    save_to_s3,
    get_s3_data_paths,
    load_s3_data,
)
from dap_prinz_green_jobs import BUCKET_NAME, config
from dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils import (
    SkillMeasures,
)
from dap_prinz_green_jobs.getters.ojo_getters import (
    get_ojo_sample,
)

from toolz import partition_all

from tqdm import tqdm
import pandas as pd

from argparse import ArgumentParser
from datetime import datetime as date

import os
import numpy as np

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--production", action="store_true", default=False)
    parser.add_argument("--job_desc_column", default="description", type=str)
    parser.add_argument("--id_column", default="id", type=str)
    parser.add_argument("--test_n", default=100, type=int)

    args = parser.parse_args()
    production = args.production
    id_column = args.id_column
    test_n = args.test_n
    job_desc_column = args.job_desc_column

    if not production:
        chunk_size = 20
    else:
        chunk_size = 10000

    print("loading datasets...")
    ojo_jobs_data = get_ojo_sample()

    # The format used in SkillMeasures
    ojo_jobs_data = ojo_jobs_data[[id_column, job_desc_column]].to_dict(
        orient="records"
    )

    if not production:
        ojo_jobs_data = ojo_jobs_data[:test_n]

    date_stamp = str(date.today().date()).replace("-", "")
    folder_name = f"outputs/data/ojo_application/extracted_green_measures/{date_stamp}/"

    skills_output_folder = f"outputs/data/green_skill_lists/{date_stamp}"

    # Skills config variables
    skills_config_name = config["skills"]["skills_config_name"]
    load_skills = config["skills"][
        "load_skills"
    ]  # Set to false if your job adverts or NER model changes
    load_skills_embeddings = config["skills"][
        "load_skills_embeddings"
    ]  # Set to false if your job advert data, NER model or way to embed changes
    load_taxonomy_embeddings = config["skills"][
        "load_taxonomy_embeddings"
    ]  # Set to false if your input taxonomy data or way to embed changes
    green_skills_classifier_model_file_name = config["skills"][
        "green_skills_classifier_model_file_name"
    ]

    if config["skills"]["load_taxonomy_embeddings"]:
        green_tax_embedding_path = config["skills"]["green_tax_embedding_path"]
    else:
        green_tax_embedding_path = os.path.join(
            skills_output_folder, "green_esco_embeddings.json"
        )

    sm = SkillMeasures(
        config_name="extract_green_skills_esco",
        green_skills_classifier_model_file_name=green_skills_classifier_model_file_name,
    )
    sm.initiate_extract_skills(local=False, verbose=True)

    taxonomy_skills_embeddings_dict = sm.get_green_taxonomy_embeddings(
        output_path=green_tax_embedding_path,
        load=load_taxonomy_embeddings,
    )

    job_desc_chunks = list(partition_all(chunk_size, ojo_jobs_data))

    print(
        f"Finding skills information for {chunk_size} job adverts in {len(job_desc_chunks)} batches."
    )

    for i, job_desc_chunk in tqdm(enumerate(job_desc_chunks)):
        skills_output = os.path.join(
            skills_output_folder, f"predicted_skills_production_{production}/{i}.json"
        )
        skill_embeddings_output = os.path.join(
            skills_output_folder,
            f"extracted_skills_embeddings_production_{production}/{i}.json",
        )

        # Where to output the mappings of skills to all of ESCO (not just green)
        skill_mappings_output_path = os.path.join(
            skills_output_folder,
            f"full_esco_skill_mappings_production_{production}/{i}.json",
        )

        prop_green_skills = sm.get_measures(
            job_desc_chunk,
            skills_output_path=skills_output,
            load_skills=load_skills,
            job_text_key=job_desc_column,
            job_id_key=id_column,
            skill_embeddings_output_path=skill_embeddings_output,
            load_skills_embeddings=load_skills_embeddings,
            skill_mappings_output_path=skill_mappings_output_path,
        )

        save_to_s3(
            BUCKET_NAME,
            prop_green_skills,
            os.path.join(
                skills_output_folder,
                f"ojo_large_sample_skills_green_measures_production_{production}_interim/{i}.json",
            ),
        )

    # Read them back in and save altogether
    prop_green_skills_locs = get_s3_data_paths(
        BUCKET_NAME,
        os.path.join(
            skills_output_folder,
            f"ojo_large_sample_skills_green_measures_production_{production}_interim",
        ),
        file_types=["*.json"],
    )

    print("Load green measures per job advert")
    all_prop_green_skills = {}
    for prop_green_skills_loc in tqdm(prop_green_skills_locs):
        all_prop_green_skills.update(load_s3_data(BUCKET_NAME, prop_green_skills_loc))

    save_to_s3(
        BUCKET_NAME,
        all_prop_green_skills,
        os.path.join(
            folder_name,
            f"ojo_large_sample_skills_green_measures_production_{production}.json",
        ),
    )
