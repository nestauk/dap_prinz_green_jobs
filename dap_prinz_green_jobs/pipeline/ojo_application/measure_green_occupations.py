"""
Create a dataset of green occupations for the sample of OJO data
"""

import pandas as pd

from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupation_measures_utils import *
from dap_prinz_green_jobs.getters.ojo import get_ojo_sample

if __name__ == "__main__":
    ojo_data = get_ojo_sample()
    jobtitle_soc_data = load_job_title_soc()
    green_soc_data = load_green_soc()

    ojo_data["job_title_cleaned"] = ojo_data["job_title_raw"].apply(
        lambda x: clean_job_title(str(x))
    )

    soc_2020_6_mapper = create_job_title_soc_mapper(
        jobtitle_soc_data, soc_column_name="soc_6_2020"
    )
    soc_2010_4_mapper = create_job_title_soc_mapper(jobtitle_soc_data)

    ojo_data["soc_6_2020"] = ojo_data["job_title_cleaned"].map(soc_2020_6_mapper)
    ojo_data["soc_4_2010"] = ojo_data["job_title_cleaned"].map(soc_2010_4_mapper)

    ojo_data_green = ojo_data.merge(green_soc_data, how="left", on="soc_4_2010")

    print(ojo_data_green["Green/Non-green"].value_counts(dropna=False))

    print(ojo_data_green["Green Category"].value_counts(dropna=False))

    print(
        ojo_data_green.groupby(["Green Category"])["job_title_cleaned"]
        .value_counts()
        .groupby(level=0, group_keys=False)
        .head(10)
    )
