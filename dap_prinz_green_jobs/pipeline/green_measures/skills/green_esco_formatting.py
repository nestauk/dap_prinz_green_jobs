"""
ESCO-specific formating to get ESCO green skills data in the format needed for the Skills Extractor,
as well as calcluating the embeddings

python dap_prinz_green_jobs/pipeline/green_measures/skills/green_esco_formatting.py
"""
from dap_prinz_green_jobs import (
    BUCKET_NAME,
    logger,
)
from dap_prinz_green_jobs.utils.bert_vectorizer import get_embeddings
from dap_prinz_green_jobs.getters.data_getters import (
    load_s3_data,
    save_to_s3,
)
from datetime import datetime as date

if __name__ == "__main__":
    date_stamp = str(date.today().date()).replace("-", "")

    # download esco green skills list
    logger.info("downloading esco green skills taxonomy")
    esco_green_skills = load_s3_data(
        BUCKET_NAME,
        "inputs/data/green_skill_lists/esco/greenSkillsCollection_en.csv",
    ).assign(id=lambda x: x["conceptUri"].str.split("/").str[-1])

    # Format needed for other steps
    esco_green_skills = esco_green_skills[["id", "preferredLabel"]].rename(
        columns={"preferredLabel": "description"}
    )
    esco_green_skills.reset_index(inplace=True)

    logger.info("uploading formatted esco green skills taxonomy")
    save_to_s3(
        BUCKET_NAME,
        esco_green_skills,
        f"outputs/data/green_skill_lists/green_esco_data_formatted_{date_stamp}.csv",
    )

    logger.info("Calculate green skills embeddings")
    taxonomy_skills_embeddings_dict = get_embeddings(
        esco_green_skills["description"].to_list(),
        id_list=list(esco_green_skills.index),
    )

    save_to_s3(
        BUCKET_NAME,
        taxonomy_skills_embeddings_dict,
        f"outputs/data/green_skill_lists/green_esco_embeddings_{date_stamp}.json",
    )
