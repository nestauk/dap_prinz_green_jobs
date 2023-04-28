"""
This script adds custom config files and formatted green skills list to the ojd-daps-skills library folder.

python dap_prinz_green_jobs/pipeline/green_measures/skills/customise_skills_extractor.py --config_name "extract_green_skills_esco"
"""
from dap_prinz_green_jobs.getters.data_getters import (
    get_s3_resource,
    load_s3_data,
    save_json_dict,
)
import shutil
import os
import ojd_daps_skills
from ojd_daps_skills.getters import download_public_data

from dap_prinz_green_jobs import BUCKET_NAME, PROJECT_DIR, get_yaml_config, logger
import argparse

s3 = get_s3_resource()

parser = argparse.ArgumentParser(
    description="Add custom config and data files to ojd-daps-skills library folder",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "--config_name",
    default="extract_green_skills_esco",
    type=str,
    help="name of config file",
)

extract_skills_library_path = ojd_daps_skills.__file__.split("/__init__.py")[0]

args = parser.parse_args()

custom_config = get_yaml_config(
    PROJECT_DIR / f"dap_prinz_green_jobs/config/{args.config_name}.yaml"
)

if __name__ == "__main__":
    # download the public data
    download_public_data.download()
    # copy the custom config file to the extract skills location
    logger.info("copying custom config file to extract skills location...")
    shutil.copy(
        PROJECT_DIR / f"dap_prinz_green_jobs/config/{args.config_name}.yaml",
        f"{extract_skills_library_path}/config/{args.config_name}.yaml",
    )

    # move custom formatted esco green skills to extract skills location
    logger.info(
        "moving custom formatted esco green skills to extract skills location..."
    )
    custom_green_skills_path = os.path.join(
        "outputs/data/green_skill_lists",
        custom_config["taxonomy_path"].split("/")[-1],
    )
    formatted_esco_green_skills = load_s3_data(
        s3, BUCKET_NAME, custom_green_skills_path
    )
    formatted_esco_green_skills.to_csv(
        f"{extract_skills_library_path}_data/{custom_config['taxonomy_path']}"
    )

    if custom_config["taxonomy_embedding_file_name"] is not None:
        logger.info(
            "moving custom esco green skill embeddings to extract skills location..."
        )
        esco_green_skill_embeddings_path = os.path.join(
            "outputs/data/green_skill_lists",
            custom_config["taxonomy_embedding_file_name"].split("/")[-1],
        )
        esco_green_skill_embeddings = load_s3_data(
            s3, BUCKET_NAME, esco_green_skill_embeddings_path
        )
        save_json_dict(
            esco_green_skill_embeddings,
            f"{extract_skills_library_path}_data/{custom_config['taxonomy_embedding_file_name']}",
        )
