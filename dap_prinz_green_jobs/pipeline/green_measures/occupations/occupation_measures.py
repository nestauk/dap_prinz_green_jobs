"""
Occupational green measures for a dataset of job adverts
"""

import pandas as pd

from dap_prinz_green_jobs.pipeline.measures.occupation_measures_utils import *


def load_ojo_data() -> pd.DataFrame():
    """
    Load the OJO dataset with job title
    """

    ojo_data_orig = pd.read_csv(
        "s3://open-jobs-lake/escoe_extension/outputs/data/model_application_data/raw_job_adverts_sample.csv"
    )
    ojo_data = ojo_data_orig.copy()
    ojo_data.columns = ojo_data.iloc[0]
    ojo_data.columns = ["job_id", "date", "title", "text"]

    return ojo_data


if __name__ == "__main__":
    ojo_data = load_ojo_data()
    jobtitle_soc_data = load_job_title_soc()
    green_soc_data = load_green_soc()

    ojo_data["job_title_cleaned"] = ojo_data["title"].apply(clean_job_title)

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
