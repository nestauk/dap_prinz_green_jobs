"""
DONT NEED THIS NOW
This is a one-off script to embed:
    - a taxonomy of skills
    - the list of already extracted skills from OJO

into a vector space.

python dap_prinz_green_jobs/pipeline/green_measures/skills/green_taxonomy_embedding.py --config_name extract_green_skills_esco
"""
import os
import yaml

from dap_prinz_green_jobs import PROJECT_DIR, get_yaml_config, BUCKET_NAME, logger
from dap_prinz_green_jobs.getters.data_getters import (
    save_to_s3,
    load_s3_data,
)
from dap_prinz_green_jobs.getters.ojo_getters import get_ojo_skills_sample
from dap_prinz_green_jobs.utils.bert_vectorizer import BertVectorizer
from dap_prinz_green_jobs.utils.processing import list_chunks
from tqdm import tqdm

from argparse import ArgumentParser
import numpy as np

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
    formatted_taxonomy = load_s3_data(BUCKET_NAME, formatted_taxonomy_path)

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
    save_to_s3(BUCKET_NAME, taxonomy_skills_embeddings_dict, embedding_path)

    # Do the same for already extracted skills in ojo_sample so we're not constantly
    # re-embedding the same skills
    logger.info("loading already extracted skills from ojo_sample...")
    ojo_skills_list = (
        get_ojo_skills_sample()
        # drop in skill_label
        .dropna(subset=["skill_label"])
        .skill_label.unique()
        .tolist()
    )

    all_extracted_skills_embeddings = []
    for batch_texts in tqdm(list_chunks(ojo_skills_list, 10000)):
        all_extracted_skills_embeddings.append(bert_model.transform(batch_texts))
    all_extracted_skills_embeddings = np.concatenate(all_extracted_skills_embeddings)

    # create dict
    all_extracted_skills_embeddings_dict = dict(
        zip(ojo_skills_list, all_extracted_skills_embeddings)
    )

    # save to s3
    save_to_s3(
        BUCKET_NAME,
        all_extracted_skills_embeddings_dict,
        "outputs/data/green_skill_lists/extracted_skills_embeddings.json",
    )
