"""
Create a dataset of green occupations for the sample of OJO data

This script acts as a way to test the occupations part of the pipeline out
"""

from collections import Counter

import pandas as pd

from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupations_measures_utils import *
from dap_prinz_green_jobs.getters.ojo_getters import get_ojo_job_title_sample
from dap_prinz_green_jobs.pipeline.green_measures.green_measures import GreenMeasures

if __name__ == "__main__":
    gm = GreenMeasures()

    ojo_job_title_raw = get_ojo_job_title_sample()

    ojo_sample = list(
        (
            ojo_job_title_raw[["job_title_raw", "id"]]
            .rename(
                columns={
                    "job_title_raw": gm.job_title_key,
                }
            )
            .T.to_dict()
            .values()
        )
    )

    ojo_sample = ojo_sample[0:100]  # For demo

    green_occupation_outputs = gm.get_occupation_measures(job_advert=ojo_sample)

    print(Counter([g["GREEN CATEGORY"] for g in green_occupation_outputs]))

    print(Counter([g["GREEN/NOT GREEN"] for g in green_occupation_outputs]))

    job_soc = []
    for i, job in enumerate(ojo_sample):
        job_info = job.copy()
        job_info.update(green_occupation_outputs[i])
        job_soc.append(job_info)

    ojo_data_green = pd.DataFrame(job_soc)

    print(
        ojo_data_green.groupby(["GREEN CATEGORY"])["job_title"]
        .value_counts()
        .groupby(level=0, group_keys=False)
        .head(10)
    )

    print(ojo_data_green.groupby(["GREEN CATEGORY"])["GREEN TIMESHARE"].mean())
