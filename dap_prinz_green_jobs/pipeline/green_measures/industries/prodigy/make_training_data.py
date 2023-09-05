"""
This script generates training data of train_size size.

For example,

    python -m dap_prinz_green_jobs.pipeline.green_measures.industries.prodigy.make_training_data --train_size 1000 --random_seed 64
"""
from dap_prinz_green_jobs import BUCKET_NAME, PROJECT_DIR, logger
from dap_prinz_green_jobs.getters.ojo_getters import get_mixed_ojo_sample
import dap_prinz_green_jobs.utils.text_cleaning as tc

from datetime import datetime
from argparse import ArgumentParser
import os
import json

import boto3

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--train_size", type=int, default=5000)
    parser.add_argument("--random_seed", type=int, default=42)

    args = parser.parse_args()

    train_size = args.train_size
    random_seed = args.random_seed

    mixed_ojo = (
        get_mixed_ojo_sample()
        .drop_duplicates(subset="description")
        .sample(frac=1, random_state=random_seed)[:train_size]  # shuffle
        .reset_index(drop=True)
    )

    mixed_ojo["clean_description"] = (
        mixed_ojo.description.apply(tc.clean_text)
        .str.replace("[", "")
        .str.replace("]", "")
        .str.strip()
    )

    training_data = mixed_ojo[["id", "clean_description"]].to_dict(orient="records")

    converted_training_data = ""
    for data in training_data:
        training_data_json = {
            "text": data["clean_description"],
            "meta": {"job_id": data["id"]},
        }
        converted_training_data += json.dumps(training_data_json, ensure_ascii=False)
        converted_training_data += "\n"

    logger.info("saving training data to s3...")
    output_dir = "inputs/data/training_data/"
    date = datetime.now().strftime("%Y-%m-%d").replace("-", "")
    s3 = boto3.client("s3")
    s3.put_object(
        Body=converted_training_data,
        Bucket=BUCKET_NAME,
        Key=os.path.join(
            output_dir, f"{date}_mixed_ojo_sample_{str(train_size)}.jsonl"
        ),
    )
