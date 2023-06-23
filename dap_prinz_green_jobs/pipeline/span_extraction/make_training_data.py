"""
This script generates training data and saves training data/examples locally if local=True and examples=True.

python -m dap_prinz_green_jobs.pipeline.span_extraction.make_training_data --train_size 5000 --local True --examples True
"""
from dap_prinz_green_jobs import BUCKET_NAME, PROJECT_DIR, logger
from dap_prinz_green_jobs.getters.ojo_getters import get_mixed_ojo_sample

from dap_prinz_green_jobs.getters.data_getters import save_to_s3, load_s3_data
import dap_prinz_green_jobs.utils.text_cleaning as tc

from argparse import ArgumentParser
import collections
import os
import json
import yaml

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--train_size", type=int, default=5000)
    parser.add_argument("--local", type=bool, default=False)
    parser.add_argument(
        "--examples",
        help="to download training examples from s3 to local directory or not",
        type=bool,
        default=False,
    )

    args = parser.parse_args()

    train_size = args.train_size
    local = args.local
    examples = args.examples

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

    training_data = list(mixed_ojo[["id", "clean_description"]].T.to_dict().values())

    converted_training_data = ""
    for data in training_data:
        training_data_json = collections.defaultdict(dict)
        training_data_json["text"] = data["clean_description"]
        training_data_json["meta"]["job_id"] = data["id"]
        converted_training_data += json.dumps(training_data_json, ensure_ascii=False)
        converted_training_data += "\n"

    output_dir = "inputs/data/training_data/"
    save_to_s3(
        BUCKET_NAME,
        converted_training_data,
        os.path.join(output_dir, f"mixed_ojo_sample_{str(train_size)}.jsonl"),
    )

    if local:
        logger.info("saving training data locally...")
        local_dir = str(
            PROJECT_DIR
            / f"dap_prinz_green_jobs/pipeline/span_extraction/data/mixed_ojo_sample_{str(train_size)}.jsonl"
        )
        # dump jsonl to local
        with open(local_dir, "w") as f:
            f.write(converted_training_data)

    if examples:
        logger.info("saving training examples locally...")
        ner_ojo_examples = load_s3_data(
            BUCKET_NAME, "inputs/data/training_data/ner_ojo.yml"
        )
        output_dir = (
            PROJECT_DIR
            / "dap_prinz_green_jobs/pipeline/span_extraction/examples/ner_ojo_examples.yml"
        )
        with open(output_dir, "w") as outfile:
            yaml.dump(ner_ojo_examples, outfile, default_flow_style=True)
