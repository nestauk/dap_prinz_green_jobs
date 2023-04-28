"""
This script cleans and generates the OJO sample used to extract green measures.

For now, its just the raw_job_adverts_sample.csv file from the OJO data lake.

python dap_prinz_green_jobs/pipeline/ojo_application/ojo_sample/make_ojo_sample.py
"""
import pandas as pd

from dap_prinz_green_jobs.getters.data_getters import save_to_s3, get_s3_resource
from dap_prinz_green_jobs import BUCKET_NAME

if __name__ == "__main__":
    s3 = get_s3_resource()

    ojo_data_orig = pd.read_csv(
        "s3://open-jobs-lake/escoe_extension/outputs/data/model_application_data/raw_job_adverts_sample.csv"
    )
    ojo_data = ojo_data_orig.copy()
    ojo_data.columns = ojo_data.iloc[0]
    ojo_data.columns = ["job_id", "date", "title", "text"]

    save_to_s3(s3, BUCKET_NAME, ojo_data, "outputs/data/ojo_application/ojo_sample.csv")
