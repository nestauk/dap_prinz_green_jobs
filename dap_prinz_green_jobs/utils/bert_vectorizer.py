from sentence_transformers import SentenceTransformer
import time
from dap_prinz_green_jobs import logger
import numpy as np
import torch

from dap_prinz_green_jobs.utils.processing import list_chunks

import time
import logging
from tqdm import tqdm


def get_embeddings(
    sent_list: list, chunk_size: int = 1000, id_list: list = None
) -> dict:
    """
    Embed a list of sentences in chunks
    Args:
        sent_list: A list of sentences
        chunk_size: The number of sentences to embed at a time
        id_list: The keys you want in the output dictionary, if not given then the sent_list values will be given
    Returns:
        dict: The sentence (key) and the embedding (value)
    """

    bert_model = BertVectorizer(verbose=True, multi_process=True).fit()

    embeddings = []
    for batch_texts in tqdm(list_chunks(sent_list, chunk_size)):
        embeddings.append(bert_model.transform(batch_texts))
    embeddings = np.concatenate(embeddings)

    if not id_list:
        id_list = sent_list

    # create dict
    return dict(zip(id_list, embeddings))


class BertVectorizer:
    """
    Use a pretrained transformers model to embed sentences.
    In this form so it can be used as a step in the pipeline.
    """

    def __init__(
        self,
        bert_model_name="sentence-transformers/all-MiniLM-L6-v2",
        multi_process=False,
        batch_size=32,
        verbose=False,
    ):
        self.bert_model_name = bert_model_name
        self.multi_process = multi_process
        self.batch_size = batch_size
        self.verbose = verbose
        if self.verbose:
            logger.setLevel(logging.INFO)
        else:
            logger.setLevel(logging.ERROR)

    def fit(self, *_):
        device = torch.device(f"cuda:0" if torch.cuda.is_available() else "cpu")
        self.bert_model = SentenceTransformer(self.bert_model_name, device=device)
        self.bert_model.max_seq_length = 512
        return self

    def transform(self, texts):
        logger.info(f"Getting embeddings for {len(texts)} texts ...")
        t0 = time.time()
        if self.multi_process:
            logger.info(".. with multiprocessing")
            pool = self.bert_model.start_multi_process_pool()
            self.embedded_x = self.bert_model.encode_multi_process(
                texts, pool, batch_size=self.batch_size
            )
            self.bert_model.stop_multi_process_pool(pool)
        else:
            self.embedded_x = self.bert_model.encode(texts, batch_size=self.batch_size)
        logger.info(f"Took {time.time() - t0} seconds")
        return self.embedded_x
