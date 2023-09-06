"""
ESCO-specific formating to get ESCO green skills data in the format needed for the Skills Extractor

python dap_prinz_green_jobs/pipeline/green_measures/skills/green_esco_formatting.py
"""
from dap_prinz_green_jobs import (
    BUCKET_NAME,
    OJO_BUCKET_NAME,
    logger,
    PROJECT_DIR,
)

from dap_prinz_green_jobs.getters.data_getters import (
    load_s3_data,
    save_to_s3,
)
import pandas as pd

if __name__ == "__main__":
    # download esco green skills list
    logger.info("downloading esco green skills taxonomy")
    esco_green_skills = load_s3_data(
        BUCKET_NAME,
        "inputs/data/green_skill_lists/esco/greenSkillsCollection_en.csv",
    ).assign(id=lambda x: x["conceptUri"].str.split("/").str[-1])

    # download formatted esco skills list
    logger.info("formatting esco green skills taxonomy")
    formatted_esco_skills = load_s3_data(
        OJO_BUCKET_NAME,
        "escoe_extension/outputs/data/skill_ner_mapping/esco_data_formatted.csv",
    )

    formatted_green_esco_skills = formatted_esco_skills[
        formatted_esco_skills["id"].isin(esco_green_skills["id"])
    ].reset_index(drop=True)

    logger.info("uploading formatted esco green skills taxonomy")
    save_to_s3(
        BUCKET_NAME,
        formatted_green_esco_skills,
        "outputs/data/green_skill_lists/green_esco_data_formatted.csv",
    )
