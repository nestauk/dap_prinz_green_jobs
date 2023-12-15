"""
Run the industry measures for the large sample of job adverts.

python dap_prinz_green_jobs/pipeline/ojo_application/flows/ojo_industry_measures.py

"""

from dap_prinz_green_jobs import logger
from dap_prinz_green_jobs.getters.data_getters import (
    save_to_s3,
    get_s3_data_paths,
    load_s3_data,
)
from dap_prinz_green_jobs import BUCKET_NAME, config
from dap_prinz_green_jobs.pipeline.green_measures.industries.industries_measures import (
    IndustryMeasures,
)
from dap_prinz_green_jobs.getters.ojo_getters import (
    get_large_ojo_sample,
)

from toolz import partition_all
from tqdm import tqdm
import pandas as pd
import numpy as np

from argparse import ArgumentParser
from datetime import datetime as date
import os
import time

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--production", action="store_true", default=False)
    parser.add_argument("--job_desc_column", default="description", type=str)
    parser.add_argument("--id_column", default="id", type=str)
    parser.add_argument("--test_n", default=1000, type=int)

    args = parser.parse_args()
    production = args.production
    id_column = args.id_column
    test_n = args.test_n
    job_desc_column = args.job_desc_column

    if not production:
        chunk_size = 1000
    else:
        chunk_size = 5000

    print("loading datasets...")
    ojo_jobs_data = get_large_ojo_sample()

    # The format used in SkillMeasures
    ojo_jobs_data = (
        ojo_jobs_data[[id_column, job_desc_column]]
        .rename(
            columns={
                id_column: config["job_adverts"]["job_id_key"],
                job_desc_column: config["job_adverts"]["job_text_key"],
            }
        )
        .to_dict(orient="records")
    )

    if not production:
        ojo_jobs_data = ojo_jobs_data[:test_n]

    date_stamp = str(date.today().date()).replace("-", "")
    folder_name = f"outputs/data/ojo_application/extracted_green_measures/{date_stamp}/"

    inds_output_folder = f"outputs/data/green_industries_lists/{date_stamp}"

    im = IndustryMeasures(
        use_gpu=False, chunk_size=100
    )  # for use in a EC2 instance set these to: use_gpu = True, chunk_size = 1000
    im.load()

    job_desc_chunks = list(partition_all(chunk_size, ojo_jobs_data))

    t0 = time.time()
    for i, job_desc_chunk in tqdm(enumerate(job_desc_chunks)):
        ind_green_measures_dict = im.get_measures(job_desc_chunk)

        save_to_s3(
            BUCKET_NAME,
            ind_green_measures_dict,
            os.path.join(
                inds_output_folder,
                f"ojo_large_sample_industry_green_measures_production_{production}_interim/{i}.json",
            ),
        )

    # Read them back in and save altogether
    ind_measures_locs = get_s3_data_paths(
        BUCKET_NAME,
        os.path.join(
            inds_output_folder,
            f"ojo_large_sample_industry_green_measures_production_{production}_interim",
        ),
        file_types=["*.json"],
    )

    print("Load green measures per job advert")
    all_ind_green_measures_dict = {}
    for ind_measures_loc in tqdm(ind_measures_locs):
        all_ind_green_measures_dict.update(load_s3_data(BUCKET_NAME, ind_measures_loc))

    save_to_s3(
        BUCKET_NAME,
        all_ind_green_measures_dict,
        os.path.join(
            folder_name,
            f"ojo_large_sample_industry_green_measures_production_{production}.json",
        ),
    )

    print(f"Time taken: {time.time() - t0}")

    inds_measures_df = (
        pd.DataFrame.from_dict(all_ind_green_measures_dict, orient="index")
        .reset_index()
        .rename(columns={"index": "job_id"})
    )

    inds_df_path = os.path.join(
        BUCKET_NAME,
        folder_name,
        f"ojo_large_sample_industry_green_measures_production_{production}.csv",
    )
    inds_measures_df.to_csv(f"s3://{inds_df_path}", index=False)