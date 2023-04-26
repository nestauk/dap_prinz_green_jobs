"""
ESCO-specific formating to get ESCO green skills data in the format needed for the Skills Extractor

python dap_prinz_green_jobs/pipeline/green_skills/green_esco_formatting.py
"""
from dap_prinz_green_jobs import (
    BUCKET_NAME,
    OJO_BUCKET_NAME,
    logger,
    get_yaml_config,
    PROJECT_DIR,
)
from nesta_ds_utils.loading_saving.S3 import download_obj, upload_obj
import pandas as pd

CONFIG = get_yaml_config(PROJECT_DIR / "dap_prinz_green_jobs/config/base.yaml")

if __name__ == "__main__":
    # download esco green skills list
    logger.info("downloading esco green skills taxonomy")
    esco_green_skills = download_obj(
        BUCKET_NAME, CONFIG["esco_green_skills_path"], download_as="dataframe"
    ).assign(id=lambda x: x["conceptUri"].str.split("/").str[-1])

    # download formatted esco skills list
    logger.info("formatting esco green skills taxonomy")
    formatted_esco_skills = download_obj(
        OJO_BUCKET_NAME, CONFIG["formatted_esco_skills_path"], download_as="dataframe"
    )

    formatted_green_esco_skills = (
        formatted_esco_skills[formatted_esco_skills["id"].isin(esco_green_skills["id"])]
        .reset_index(drop=True)
        .set_index("id")
    )

    logger.info("uploading formatted esco green skills taxonomy")
    upload_obj(
        formatted_green_esco_skills,
        BUCKET_NAME,
        CONFIG["formatted_esco_green_skills_path"],
    )
