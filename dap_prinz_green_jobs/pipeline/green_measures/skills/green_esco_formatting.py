"""
ESCO-specific formating to get ESCO green skills data in the format needed for the Skills Extractor

python dap_prinz_green_jobs/pipeline/green_measures/skills/green_esco_formatting.py
"""
from dap_prinz_green_jobs import (
    BUCKET_NAME,
    OJO_BUCKET_NAME,
    logger,
    get_yaml_config,
    PROJECT_DIR,
)

from dap_prinz_green_jobs.getters.data_getters import (
    load_s3_data,
    save_to_s3,
    get_s3_resource,
)
import pandas as pd

CONFIG = get_yaml_config(PROJECT_DIR / "dap_prinz_green_jobs/config/base.yaml")
s3 = get_s3_resource()

if __name__ == "__main__":
    # download esco green skills list
    logger.info("downloading esco green skills taxonomy")
    esco_green_skills = load_s3_data(
        s3, BUCKET_NAME, CONFIG["esco_green_skills_path"]
    ).assign(id=lambda x: x["conceptUri"].str.split("/").str[-1])

    # download formatted esco skills list
    logger.info("formatting esco green skills taxonomy")
    formatted_esco_skills = load_s3_data(
        s3, OJO_BUCKET_NAME, CONFIG["formatted_esco_skills_path"]
    )

    formatted_green_esco_skills = (
        formatted_esco_skills[formatted_esco_skills["id"].isin(esco_green_skills["id"])]
        .reset_index(drop=True)
        .set_index("id")
    )

    logger.info("uploading formatted esco green skills taxonomy")
    save_to_s3(
        s3,
        BUCKET_NAME,
        formatted_green_esco_skills,
        CONFIG["formatted_esco_green_skills_path"],
    )
