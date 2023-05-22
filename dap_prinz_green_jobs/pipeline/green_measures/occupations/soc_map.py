"""
A class to map inputted job titles to their most likely SOC codes.

Usage:

soc_mapper = SOCMapper()
soc_mapper.load()
matches = soc_mapper.get_soc(job_titles=["data scientist", "Assistant nurse", "Senior financial consultant - London"])
>>> [('2425', 'data scientist'), ('6141', 'assistant nurse'), ('3534', 'financial consultant')]

"""
from collections import Counter, defaultdict
import os
from typing import List, Union

from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
from tqdm import tqdm
import numpy as np

from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupation_measures_utils import (
    load_job_title_soc,
)
from dap_prinz_green_jobs.getters.data_getters import (
    save_to_s3,
    load_s3_data,
    load_json_dict,
)
from dap_prinz_green_jobs import BUCKET_NAME, logger, config, PROJECT_DIR


def chunks(orig_list, n_chunks):
    for i in range(0, len(orig_list), n_chunks):
        yield orig_list[i : i + n_chunks]


class SOCMapper(object):
    """Class for linking job titles to SOC codes.

        The input job title is matched to a dataset of job titles with their SOC.
        - If the most similar job title is very similar, then the corresponding SOC is outputted.
        - Otherwise, we look at a group of the most similar job titles, and if they all have the same SOC, then this is outputted.

        Attributes
    ----------

        :param local: Whether to read data from a local location or not, defaults to True
    :type local: bool

    :param embeddings_output_dir: (optional) The directory the embeddings are stored, or will be stored if saved
    :type embeddings_output_dir: str, None

    :param batch_size: How many job titles per batch for embedding, defaults to 500
    :type batch_size: int

    :param match_top_n: The number of most similar SOC matches to consider when calculating the final SOC and outputing
    :type match_top_n: int

    :param sim_threshold: The similarity threshold for outputting the most similar SOC match.
    :type sim_threshold: float

    :param top_n_sim_threshold: The similarity threshold for a match being added to a group of SOC matches.
    :type top_n_sim_threshold: float

    :param minimum_n: The minimum size of a group of SOC matches.
    :type minimum_n: int

    :param minimum_prop: If a group of SOC matches have a high proportion (>= minimum_prop) of the same SOC being matched, then use this SOC.
    :type minimum_prop: float

    ----------
    Methods
    ----------

    load_process_soc_data():
        Load the SOC data
    unique_soc_job_titles(jobtitle_soc_data):
        Convert the SOC data into a dict where each key is a job title and the value is the SOC code
        embed_texts(texts):
                Get sentence embeddings for a list of input texts
        load(save_embeds=False):
                Load everything to use this class, calculate SOC embeddings if they weren't inputted, save embeddings if desired
        find_most_similar_matches(job_titles, job_title_embeddings):
                Using the inputted job title embeddings and the SOC embeddings, find the full information about the most similar SOC job titles
        find_most_likely_soc(match_row):
                For the full match information for one job title, find the most likely SOC (via top match, or group of top matches)
        get_soc(job_titles, additional_info=False):
                (main function) For inputted job titles, output the best SOC match, add extra information about matches using the additional_info argument

        ----------
    Usage
    ----------
        soc_mapper = SOCMapper()
        soc_mapper.load()
        matches = soc_mapper.get_soc(job_titles=["data scientist", "Assistant nurse", "Senior financial consultant - London"])

    """

    def __init__(
        self,
        local: bool = True,
        embeddings_output_dir: str = "outputs/data/green_occupations/soc_matching/",
        batch_size: int = 500,
        match_top_n: int = 10,
        sim_threshold: float = 0.7,
        top_n_sim_threshold: float = 0.5,
        minimum_n: int = 3,
        minimum_prop: float = 0.5,
    ):
        self.local = local
        self.embeddings_output_dir = embeddings_output_dir
        self.batch_size = batch_size
        self.match_top_n = match_top_n
        self.sim_threshold = sim_threshold
        self.top_n_sim_threshold = top_n_sim_threshold
        self.minimum_n = minimum_n
        self.minimum_prop = minimum_prop

    def load_process_soc_data(self):
        """
        Load the job titles to SOC codes dataset as found on the ONS website.
        A small amount of processing.
        """

        jobtitle_soc_data = load_job_title_soc()
        jobtitle_soc_data = jobtitle_soc_data[jobtitle_soc_data["soc_4_2010"] != "}}}}"]

        return jobtitle_soc_data

    def unique_soc_job_titles(self, jobtitle_soc_data: pd.DataFrame()) -> dict:
        """
        Taking the dataset of job titles and which SOC they belong to - create a unique
        dictionary where each key is a job title and the value is the SOC code.
        There are additional words to include in the job title if at first
        it is not unique.
        """

        col_name_0 = "INDEXOCC NATURAL WORD ORDER"
        col_name_1 = "ADD"
        col_name_2 = "IND"

        jobtitle_soc_data[f"{col_name_0} and {col_name_1}"] = jobtitle_soc_data.apply(
            lambda x: x[col_name_0] + " " + x[col_name_1]
            if pd.notnull(x[col_name_1])
            else x[col_name_0],
            axis=1,
        )
        jobtitle_soc_data[
            f"{col_name_0} and {col_name_1} and {col_name_2}"
        ] = jobtitle_soc_data.apply(
            lambda x: x[f"{col_name_0} and {col_name_1}"] + " " + x[col_name_2]
            if pd.notnull(x[col_name_2])
            else x[f"{col_name_0} and {col_name_1}"],
            axis=1,
        )

        # Try to find a unique job title to SOC 4 mapping
        job_title_2_soc6_4 = {}
        for job_title, grouped_soc_data in jobtitle_soc_data.groupby(col_name_0):
            if grouped_soc_data["soc_6_2020"].nunique() == 1:
                job_title_2_soc6_4[job_title] = (
                    grouped_soc_data["soc_6_2020"].unique()[0],
                    grouped_soc_data["soc_4_2010"].unique()[0],
                )
            else:
                for job_title_1, grouped_soc_data_1 in grouped_soc_data.groupby(
                    f"{col_name_0} and {col_name_1}"
                ):
                    if grouped_soc_data_1["soc_6_2020"].nunique() == 1:
                        job_title_2_soc6_4[job_title_1] = (
                            grouped_soc_data_1["soc_6_2020"].unique()[0],
                            grouped_soc_data_1["soc_4_2010"].unique()[0],
                        )
                    else:
                        for (
                            job_title_2,
                            grouped_soc_data_2,
                        ) in grouped_soc_data_1.groupby(
                            f"{col_name_0} and {col_name_1} and {col_name_2}"
                        ):
                            if grouped_soc_data_2["soc_6_2020"].nunique() == 1:
                                job_title_2_soc6_4[job_title_2] = (
                                    grouped_soc_data_2["soc_6_2020"].unique()[0],
                                    grouped_soc_data_2["soc_4_2010"].unique()[0],
                                )

        return job_title_2_soc6_4

    def embed_texts(
        self,
        texts: list,
    ) -> np.array(object):
        logger.info(f"Embedding texts in {len(texts)/self.batch_size} batches")
        all_embeddings = []
        for batch_texts in tqdm(chunks(texts, self.batch_size)):
            all_embeddings.append(
                self.bert_model.encode(np.array(batch_texts), batch_size=32)
            )
        all_embeddings = np.concatenate(all_embeddings)

        return all_embeddings

    def load(self, save_embeds=False):
        self.bert_model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        self.bert_model.max_seq_length = 512

        self.jobtitle_soc_data = self.load_process_soc_data()

        self.job_title_2_soc6_4 = self.unique_soc_job_titles(self.jobtitle_soc_data)

        embeddings_path = os.path.join(
            self.embeddings_output_dir, "soc_job_embeddings.json"
        )
        job_titles_path = os.path.join(
            self.embeddings_output_dir, "soc_job_embeddings_titles.json"
        )

        try:
            logger.info(f"Loading SOC job title embeddings")
            if self.local:
                self.all_soc_embeddings = load_json_dict(
                    os.path.join(PROJECT_DIR, embeddings_path)
                )
                self.soc_job_titles = load_json_dict(
                    os.path.join(PROJECT_DIR, job_titles_path)
                )
            else:
                self.all_soc_embeddings = load_s3_data(BUCKET_NAME, embeddings_path)
                self.soc_job_titles = load_s3_data(BUCKET_NAME, job_titles_path)
        except:
            logger.info(f"SOC job title embeddings not found")

            # Embed the SOC job titles
            self.soc_job_titles = list(self.job_title_2_soc6_4.keys())

            self.all_soc_embeddings = self.embed_texts(self.soc_job_titles)

            if save_embeds:
                logger.info(f"Saving SOC job title embeddings")
                save_to_s3(BUCKET_NAME, self.all_soc_embeddings, embeddings_path)
                save_to_s3(BUCKET_NAME, self.soc_job_titles, job_titles_path)

    def find_most_similar_matches(
        self,
        job_titles: Union[str, List[str]],
        job_title_embeddings: np.array(object),
    ) -> dict:
        """
        Using the job title embeddings and the SOC job title embeddings,
        find the top n SOC job titles which are most similar to each input job title.
        """

        logger.info(f"Finding most similar job titles for {len(job_titles)} job titles")

        similarities = cosine_similarity(job_title_embeddings, self.all_soc_embeddings)

        # Top matches for each data point
        job_top_soc_matches = []
        for job_title_ix, job_title in tqdm(enumerate(job_titles)):
            top_soc_matches = []
            for soc_ix in np.flip(np.argsort(similarities[job_title_ix]))[
                0 : self.match_top_n
            ]:
                soc_text = self.soc_job_titles[soc_ix]
                top_soc_matches.append(
                    [
                        soc_text,
                        self.job_title_2_soc6_4[soc_text][1],
                        similarities[job_title_ix][soc_ix],
                    ]
                )
            job_top_soc_matches.append(
                {
                    "job_title": job_title,
                    "top_soc_matches": top_soc_matches,
                }
            )

        return job_top_soc_matches

    def find_most_likely_soc(
        self,
        match_row: dict,
    ) -> tuple:
        """
        For a single job title and the details of the most similar SOC matches, find a single most likely SOC
        1. If the top match has a really high similarity score (>sim_threshold) then use this.
                This will return (soc, job_title)
        2. Get the SOCs of the good (>top_n_sim_threshold) matches in the top n most similar.
        3. If there are a few of these (>=minimum_n) and over a certain proportion (>minimum_prop) of these are the same - use this as the SOC.
                This will return (soc, the job titles given for this same soc)
        """

        top_soc_match = match_row["top_soc_matches"][0][0]
        top_soc_match_code = match_row["top_soc_matches"][0][1]
        top_soc_match_score = match_row["top_soc_matches"][0][2]

        if top_soc_match_score > self.sim_threshold:
            return (top_soc_match_code, top_soc_match)
        else:
            all_good_socs = [
                t[1]
                for t in match_row["top_soc_matches"]
                if t[2] > self.top_n_sim_threshold
            ]
            if len(all_good_socs) >= self.minimum_n:
                common_soc, num_common_soc = Counter(all_good_socs).most_common(1)[0]
                prop_most_common_soc = num_common_soc / len(all_good_socs)
                if prop_most_common_soc > self.minimum_prop:
                    return (
                        common_soc,
                        set(
                            [
                                t[0]
                                for t in match_row["top_soc_matches"]
                                if (
                                    (t[2] > self.top_n_sim_threshold)
                                    and (t[1] == common_soc)
                                )
                            ]
                        ),
                    )
                else:
                    return None
            else:
                return None

    def get_soc(self, job_titles: Union[str, List[str]], additional_info: bool = False):
        """Get the most likely SOC for each inputted job title

                :param job_titles: A single job title or a list of raw job titles
        :type job_titles: str, list of str

                :param additional_info: Whether to provide additional information about the matches.
                        Return just the most likely soc match (False) or the top soc matches (True)
        :type additional_info: bool

        :return: A list of the top matches for each job title inputted
        :rtype: list

        """

        if isinstance(job_titles, str):
            job_titles = [job_titles]

        # Embed the input job titles
        job_title_embeddings = self.embed_texts(job_titles)

        top_soc_matches = self.find_most_similar_matches(
            job_titles, job_title_embeddings
        )

        logger.info(f"Finding most likely SOC")
        found_count = 0
        for job_matches in top_soc_matches:
            most_likely_soc4 = self.find_most_likely_soc(job_matches)
            job_matches["most_likely_soc4"] = most_likely_soc4
            if most_likely_soc4:
                found_count += 1

        logger.info(
            f"Found SOCs for {found_count*100/len(top_soc_matches)}% of the job titles"
        )

        if additional_info:
            return top_soc_matches
        else:
            return [
                job_matches.get("most_likely_soc4") for job_matches in top_soc_matches
            ]
