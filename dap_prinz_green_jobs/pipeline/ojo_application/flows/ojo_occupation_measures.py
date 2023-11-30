"""
Run the occupation measures for the large sample of job adverts.

python -i dap_prinz_green_jobs/pipeline/ojo_application/flows/ojo_occupation_measures.py

Took about 1h30m
"""
from tqdm import tqdm

import os
from toolz import partition_all
from datetime import datetime as date

from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupations_measures_utils import (
    OccupationMeasures,
)
from dap_prinz_green_jobs import config, BUCKET_NAME
from dap_prinz_green_jobs.getters.ojo_getters import (
    get_large_ojo_job_title_sample,
)
from dap_prinz_green_jobs.getters.data_getters import (
    save_to_s3,
    get_s3_data_paths,
    load_s3_data,
)

if __name__ == "__main__":
    production = True
    job_title_column = "job_title_raw"
    id_column = "id"

    date_stamp = str(date.today().date()).replace("-", "")
    folder_name = f"outputs/data/ojo_application/extracted_green_measures/{date_stamp}/"

    om = OccupationMeasures()
    om.load(
        local=config["occupations"]["local"],
        embeddings_output_dir=config["occupations"]["embeddings_output_dir"] + "flow/",
        batch_size=config["occupations"]["batch_size"],
        match_top_n=config["occupations"]["match_top_n"],
        sim_threshold=config["occupations"]["sim_threshold"],
        top_n_sim_threshold=config["occupations"]["top_n_sim_threshold"],
        minimum_n=config["occupations"]["minimum_n"],
        minimum_prop=config["occupations"]["minimum_prop"],
        save_embeds=config["occupations"]["save_embeds"],
    )

    soc_name_dict = {
        "soc_2020_6": om.soc_mapper.soc_2020_6_dict,
        "soc_2020_4": om.soc_mapper.soc_2020_4_dict,
    }

    save_to_s3(
        BUCKET_NAME,
        soc_name_dict,
        os.path.join(folder_name, f"soc_name_dict.json"),
    )

    if not production:
        test_sample_n = 10
        chunk_size = 5  # Number of job titles in each extract_soc step
    else:
        chunk_size = 10000

    print("loading datasets...")
    ojo_jobs_data = get_large_ojo_job_title_sample()

    # The format used in OccupationMeasures
    ojo_jobs_data = ojo_jobs_data[[id_column, job_title_column]].to_dict(
        orient="records"
    )

    if not production:
        ojo_jobs_data = ojo_jobs_data[:test_sample_n]

    unique_job_titles = list(
        set(
            [
                job.get(job_title_column)
                for job in ojo_jobs_data
                if job_title_column in job
            ]
        )
    )

    print(
        f"there are {len(unique_job_titles)} unique job titles to extract SOC codes for..."
    )

    job_title_chunks = list(partition_all(chunk_size, unique_job_titles))

    print(
        f".. finding SOC information for these in {len(job_title_chunks)} batches of {chunk_size} job titles each."
    )

    print("Extract SOC codes for unique job titles")

    for i, job_title_chunk in tqdm(enumerate(job_title_chunks)):
        job_title_2_match = om.precalculate_soc_mapper(job_title_chunk)
        save_to_s3(
            BUCKET_NAME,
            job_title_2_match,
            os.path.join(
                folder_name,
                f"ojo_large_sample_jobtitles2soc_production_{str(production).lower()}/{i}.json",
            ),
        )

    job_title_2_match_locs = get_s3_data_paths(
        BUCKET_NAME,
        os.path.join(
            folder_name,
            f"ojo_large_sample_jobtitles2soc_production_{str(production).lower()}",
        ),
        file_types=["*.json"],
    )

    print("Load job title to SOC")
    all_job_title_2_match = {}
    for job_title_2_match_loc in tqdm(job_title_2_match_locs):
        all_job_title_2_match.update(load_s3_data(BUCKET_NAME, job_title_2_match_loc))

    om.job_title_2_match = all_job_title_2_match

    job_ad_chunks = list(partition_all(chunk_size, ojo_jobs_data))

    print(
        f"Finding green measures information for {len(job_ad_chunks)} batches of {chunk_size} job adverts each."
    )

    all_green_occupation_measures_dict = {}

    for job_ad_chunk in tqdm(job_ad_chunks):
        occ_green_measures_list = om.get_measures(
            job_adverts=job_ad_chunk, job_title_key=job_title_column
        )

        green_occupation_measures_dict = dict(
            zip([j[id_column] for j in job_ad_chunk], occ_green_measures_list)
        )
        all_green_occupation_measures_dict.update(green_occupation_measures_dict)

    print("saving to s3...")

    save_to_s3(
        BUCKET_NAME,
        all_green_occupation_measures_dict,
        os.path.join(
            folder_name,
            f"ojo_large_sample_occupation_green_measures_production_{str(production).lower()}.json",
        ),
    )
