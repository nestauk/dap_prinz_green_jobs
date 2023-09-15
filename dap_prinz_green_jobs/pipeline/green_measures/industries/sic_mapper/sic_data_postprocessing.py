"""
Post-process of SIC company descriptions data to
    create a FAISS index and save it to disk.

python dap_prinz_green_jobs/pipeline/green_measures/industries/sic_mapper/sic_data_postprocessing.py
"""
import faiss

import yaml
import os
from dap_prinz_green_jobs import PROJECT_DIR, logger, BUCKET_NAME
from dap_prinz_green_jobs.getters.data_getters import save_to_s3
from dap_prinz_green_jobs.utils.bert_vectorizer import BertVectorizer
import pandas as pd

# load config
config_path = os.path.join(PROJECT_DIR, "dap_prinz_green_jobs/config/base.yaml")
with open(config_path, "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

sic_company_descriptions_path = config["industries"]["sic_comp_desc_path"]
data_outputs_path = config["job_adverts"]["data_folder_name"]


sic_df_grouped = pd.read_csv(
    f"s3://{BUCKET_NAME}/{data_outputs_path}green_industries/{sic_company_descriptions_path.replace('json', 'csv')}"
)

if __name__ == "__main__":
    logger.info("creating and saving FAISS index...")

    bert_model_name = f"sentence-transformers/{config['industries']['bert_model_name']}"
    bert_model = BertVectorizer(
        bert_model_name=bert_model_name,
        multi_process=config["industries"]["multi_process"],
    ).fit()

    sic_embeds = bert_model.transform(
        list(sic_df_grouped.sic_company_description.tolist())
    )

    d = sic_embeds.shape[1]  # define the dimensionality of the vectors
    # lets use brute force L2
    llm_index = faiss.IndexFlatL2(d)
    faiss.normalize_L2(sic_embeds)  # normalize vectors
    llm_index.add(sic_embeds)  # add vectors to the index

    faiss_index_path = f"{config['job_adverts']['data_folder_name']}/green_industries"

    full_faiss_index_path = os.path.join(
        faiss_index_path,
        f"{sic_company_descriptions_path}.index",
    )
    # if it doesn't exist, create it
    if not os.path.exists(faiss_index_path):
        logger.info(f"{faiss_index_path} directory does not exist. Creating it now...")
        os.makedirs(faiss_index_path)
    faiss.write_index(llm_index, full_faiss_index_path)
