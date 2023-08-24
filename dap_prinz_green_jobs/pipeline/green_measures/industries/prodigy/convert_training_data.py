"""
Convert NER jsonl training data to binary sentence labels
    to fine tune a huggingface transformer model.

To save locally:
    python dap_prinz_green_jobs/pipeline/green_measures/industries/prodigy/convert_training_data.py -f company_desc_sic_labelled_170823.jsonl -l

else don't use -l flag
"""
import plac
import os
import pandas as pd
import srsly
from spacy.tokens import Span
import spacy

from dap_prinz_green_jobs import PROJECT_DIR, logger


industries_path = "dap_prinz_green_jobs/pipeline/green_measures/industries"
training_data_path = os.path.join(
    PROJECT_DIR, industries_path, "prodigy/data/labelled_data/"
)


@plac.annotations(
    file_name=("Name of JSONL input file", "option", "f", str),
    local=("Whether to save the data locally or not.", "flag", "l", bool),
)
def convert_training_data(
    file_name="company_desc_sic_labelled_170823.jsonl", local=False
):
    training_dataset = os.path.join(training_data_path, file_name)
    nlp = spacy.load("en_core_web_sm")

    logger.info("converting NER labels to sentence labels...")
    training_data_list = []
    for line in srsly.read_jsonl(training_dataset):
        if line["answer"] == "accept":
            doc = nlp(line["text"])
            job_id = line["meta"]["job_id"]
            all_sents = list(doc.sents)
            comp_sents = []
            if "spans" in line:
                for span in line["spans"]:
                    spans = Span(
                        doc,
                        span["token_start"],
                        span["token_end"] + 1,
                        span["label"],
                    )
                    comp_sents.append(spans.sent)
                comp_sent_labels = [
                    1 if sent in comp_sents else 0 for sent in all_sents
                ]
                training_data_list.extend(
                    [
                        tuple([job_id, sent, label])
                        for sent, label in zip(all_sents, comp_sent_labels)
                    ]
                )

    training_data_df = pd.DataFrame(
        training_data_list, columns=["job_id", "sentence", "label"]
    )
    output_filename = file_name.split(".")[0] + "_sentences.csv"

    if local:
        logger.info(f"saving {output_filename} locally...")
        training_data_df.to_csv(f"{training_data_path}/{output_filename}", index=False)
    else:
        logger.info(f"saving {output_filename} training data to s3...")
        training_data_df.to_csv(
            f"s3://prinz-green-jobs/outputs/data/labelled_job_adverts/{output_filename}",
            index=False,
        )


if __name__ == "__main__":
    plac.call(convert_training_data)
