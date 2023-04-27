"""
Getters for data related to OJO job adverts
"""
import pandas as pd
from dap_prinz_green_jobs import BUCKET_NAME
from dap_prinz_green_jobs.getters.data_getters import get_s3_resource, load_s3_data

s3 = get_s3_resource()


# to remove any cleaning from this step
def get_ojo_sample() -> pd.DataFrame:
    """Gets ojo sample data from s3

    Returns:
        pd.Dataframe: ojo sample data
    """
    return load_s3_data(s3, BUCKET_NAME, "outputs/data/ojo_application/ojo_sample.csv")
