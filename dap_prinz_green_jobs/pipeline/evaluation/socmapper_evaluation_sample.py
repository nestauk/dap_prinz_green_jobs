"""
Create a sample dataset to evaluate how well occupations were linked to SOC
"""

import pandas as pd
import pyarrow.parquet as pq

from tqdm import tqdm

from dap_prinz_green_jobs.getters.data_getters import save_to_s3
from dap_prinz_green_jobs import BUCKET_NAME, logger, config
from dap_prinz_green_jobs.pipeline.green_measures.occupations.soc_map import SOCMapper
from dap_prinz_green_jobs.getters.occupation_getters import load_job_title_soc
from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupations_data_processing import (
    process_job_title_soc,
)

if __name__ == "__main__":
    # Get the most common job titles in OJO

    adverts_ojd_daps_extract = pd.read_parquet(
        config["ojo_s3_file_adverts_ojd_daps_extract"]
    )

    count_job_titles = adverts_ojd_daps_extract["job_title_raw"].value_counts()
    print(
        f"There are a total of {len(count_job_titles)} unique job titles in all of OJO"
    )
    common_job_titles = count_job_titles[0:1000].to_dict()

    # Get the SOC name

    jobtitle_soc_data = process_job_title_soc(load_job_title_soc())
    soc_2020_ext_dict = dict(
        zip(
            jobtitle_soc_data["SOC_2020_EXT"],
            jobtitle_soc_data["SUB-UNIT GROUP DESCRIPTIONS"],
        )
    )
    soc_2020_dict = dict(
        zip(
            [int(s) for s in jobtitle_soc_data["SOC_2020"]],
            jobtitle_soc_data["SOC 2020 UNIT GROUP DESCRIPTIONS"],
        )
    )

    # Use SOCmapper

    soc_mapper = SOCMapper()
    soc_mapper.load()
    socs = soc_mapper.get_soc(job_titles=list(common_job_titles.keys()))
    socs_additional_info = soc_mapper.get_soc(
        job_titles=list(common_job_titles.keys()), additional_info=True
    )

    # Output
    evaluation_data = []
    for (ojo_job_title, num_job_ads), soc_info in zip(
        common_job_titles.items(), socs_additional_info
    ):
        if soc_info["most_likely_soc"]:
            if type(soc_info["most_likely_soc"][0]) == str:
                soc_2020_4, occ_matched = soc_info["most_likely_soc"]
                soc_2020_6 = None
                soc_2010_4 = None
                match_type = "Multiple 4-digit 2020 socs"
                match_prob = None
            else:
                ((soc_2020_6, soc_2020_4, soc_2010_4), occ_matched) = soc_info[
                    "most_likely_soc"
                ]
                match_type = "Top 6-digit 2020 soc"
                match_prob = soc_info["top_soc_matches"][0][4]

            soc_2020_4 = int(soc_2020_4)
        else:
            soc_2020_6 = None
            soc_2020_4 = None
            soc_2010_4 = None
            occ_matched = None
            match_type = "None found"
            match_prob = None
        evaluation_data.append(
            {
                "ojo_job_title": ojo_job_title,
                "num_job_ads": num_job_ads,
                "prop_job_ads": num_job_ads / len(adverts_ojd_daps_extract),
                "soc_2020_6": soc_2020_6,
                "soc_2020_6_name": soc_2020_ext_dict.get(soc_2020_6),
                "soc_2020_4": soc_2020_4,
                "soc_2020_4_name": soc_2020_dict.get(soc_2020_4),
                "soc_2010_4": soc_2010_4,
                "occ_matched": occ_matched,
                "match_type": match_type,
                "match_prob": match_prob,
            }
        )

    evaluation_data_df = pd.DataFrame(evaluation_data)

    evaluation_data_df.to_csv(
        "dap_prinz_green_jobs/pipeline/evaluation/soc_evaluation_sample.csv"
    )
