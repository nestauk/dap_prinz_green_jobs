"""
This is a one-of script to embed a taxonomy of skills into a vector space.

python dap_prinz_green_jobs/pipeline/green_skills/green_taxonomy_embedding.py --config_name extract_green_skills_esco
"""
import os
import yaml

from dap_prinz_green_jobs import PROJECT_DIR, get_yaml_config, BUCKET_NAME
from dap_prinz_green_jobs.getters.data_getters import (
    save_to_s3,
    load_s3_data,
    get_s3_resource,
)
from dap_prinz_green_jobs.utils.bert_vectorizer import BertVectorizer

from argparse import ArgumentParser
import numpy as np

s3 = get_s3_resource()

if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument(
        "--config_name",
        help="taxonomy config name",
        default="extract_green_skills_esco",
    )

    args = parser.parse_args()

    config_name = args.config_name
    config = get_yaml_config(
        PROJECT_DIR / f"dap_prinz_green_jobs/config/{config_name}.yaml"
    )

    formatted_taxonomy_path = config["taxonomy_path"].replace(
        "skill_ner_mapping", "green_skill_lists"
    )

    embedding_path = config["taxonomy_embedding_file_name"].replace(
        "skill_ner_mapping", "green_skill_lists"
    )

    # load formatted taxonomy path
    formatted_taxonomy = load_s3_data(s3, BUCKET_NAME, formatted_taxonomy_path)

    # instantiate bert model
    bert_model = BertVectorizer(verbose=True, multi_process=True).fit()

    # embed clean skills
    taxonomy_skills_embeddings = bert_model.transform(
        formatted_taxonomy["description"].to_list()
    )

    # create dict
    taxonomy_skills_embeddings_dict = dict(
        zip(list(formatted_taxonomy.index), np.array(taxonomy_skills_embeddings))
    )

    # save to s3
    save_to_s3(s3, BUCKET_NAME, taxonomy_skills_embeddings_dict, embedding_path)
