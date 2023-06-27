"""
This script generates training data of train_size size.

For example,

    python -m dap_prinz_green_jobs.pipeline.span_extraction.make_training_data --train_size 5000
"""
from dap_prinz_green_jobs import BUCKET_NAME, PROJECT_DIR, logger
from dap_prinz_green_jobs.getters.ojo_getters import get_mixed_ojo_sample

from dap_prinz_green_jobs.getters.data_getters import save_to_s3, load_s3_data
import dap_prinz_green_jobs.utils.text_cleaning as tc

from argparse import ArgumentParser
import os
import json

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--train_size", type=int, default=5000)

    args = parser.parse_args()

    train_size = args.train_size

    mixed_ojo = (
        get_mixed_ojo_sample()
        .drop_duplicates(subset="description")
        .sample(frac=1, random_state=42)[:train_size]  # shuffle
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
    save_to_s3(
        BUCKET_NAME,
        converted_training_data,
        os.path.join(output_dir, f"mixed_ojo_sample_{str(train_size)}.jsonl"),
    )