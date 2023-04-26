"""
Getters for data related to ojo job adverts
"""
import pandas as pd


# to remove any cleaning from this step
def get_ojo_sample() -> pd.DataFrame:
    """Gets ojo sample data from s3

    Returns:
        pd.Dataframe: ojo sample data
    """
    ojo_data_orig = pd.read_csv(
        "s3://open-jobs-lake/escoe_extension/outputs/data/model_application_data/raw_job_adverts_sample.csv"
    )
    ojo_data = ojo_data_orig.copy()
    ojo_data.columns = ojo_data.iloc[0]
    ojo_data.columns = ["job_id", "date", "title", "text"]

    return ojo_data
