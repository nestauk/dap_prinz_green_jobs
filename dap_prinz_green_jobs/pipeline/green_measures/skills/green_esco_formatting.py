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

import pandas as pd

from datetime import datetime as date

if __name__ == "__main__":
    date_stamp = str(date.today().date()).replace("-", "")

    # download esco green skills list
    logger.info("downloading esco green skills taxonomy")
    esco_green_skills = load_s3_data(
        BUCKET_NAME,
        "inputs/data/green_skill_lists/esco/greenSkillsCollection_en.csv",
    ).assign(id=lambda x: x["conceptUri"].str.split("/").str[-1])

    logger.info("Formatting the esco green skills taxonomy")

    esco_green_skills["type"] = "preferredLabel"

    # Expand out the alternate labels
    esco_green_skills["altLabels_expanded"] = esco_green_skills["altLabels"].apply(
        lambda x: x.split("|") if isinstance(x, str) else None
    )
    esco_green_skills_exploded = esco_green_skills.explode("altLabels_expanded")
    esco_green_skills_exploded["type"] = "altLabel"

    # Add the preferred labels and the alternate together in the format needed for other steps
    expanded_green_esco = pd.concat(
        [
            esco_green_skills[["id", "preferredLabel", "type", "skillType"]].rename(
                columns={"preferredLabel": "description"}
            ),
            esco_green_skills_exploded[
                ["id", "altLabels_expanded", "type", "skillType"]
            ].rename(columns={"altLabels_expanded": "description"}),
        ]
    )
    # Don't include blank descriptions
    expanded_green_esco = expanded_green_esco[
        pd.notnull(expanded_green_esco["description"])
    ]

    # The new descriptions often have trailing whitespaces
    expanded_green_esco["description"] = expanded_green_esco["description"].apply(
        lambda x: x.strip()
    )

    # There are times when the preferred label and the altlabel descriptions are the same for the same id, keep the preferred only
    deduplicated_expanded_green_esco = (
        expanded_green_esco.groupby(["id", "description"], sort=False)
        .apply(
            lambda x: x
            if "preferredLabel" not in x["type"].tolist()
            else x.loc[x.type.eq("preferredLabel")]
        )
        .reset_index(drop=True)
    )

    # There are times when the description is the same but the id isn't, we need to just have one, so keep the one
    # from skillType = "skill/competence" (rather than "knowledge") if possible otherwise pick the first one.
    # Note this only happens 19 times
    deduplicated_expanded_green_esco = (
        deduplicated_expanded_green_esco.groupby(["description"], sort=False)
        .apply(
            lambda x: x.iloc[0]
            if "skill/competence" not in x["skillType"].tolist()
            else x.loc[x["skillType"].eq("skill/competence")].iloc[0]
        )
        .reset_index(drop=True)
    )

    deduplicated_expanded_green_esco.reset_index(inplace=True)
    logger.info(
        f"{len(deduplicated_expanded_green_esco)} green skill descriptions from {deduplicated_expanded_green_esco['id'].nunique()} unique green skills"
    )

    logger.info("uploading formatted esco green skills taxonomy")
    save_to_s3(
        BUCKET_NAME,
        deduplicated_expanded_green_esco,
        f"outputs/data/green_skill_lists/green_esco_data_formatted_{date_stamp}.csv",
    )

    logger.info("Calculate green skills embeddings")
    taxonomy_skills_embeddings_dict = get_embeddings(
        deduplicated_expanded_green_esco["description"].to_list(),
        id_list=list(deduplicated_expanded_green_esco.index),
    )

    save_to_s3(
        BUCKET_NAME,
        taxonomy_skills_embeddings_dict,
        f"outputs/data/green_skill_lists/green_esco_embeddings_{date_stamp}.json",
    )
