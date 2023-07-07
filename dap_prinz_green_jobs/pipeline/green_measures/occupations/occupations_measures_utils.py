"""
Utils for occuational green measures
"""

import pandas as pd

from typing import List, Union

from dap_prinz_green_jobs.getters.occupation_getters import (
    load_green_gla_soc,
    load_green_timeshare_soc,
    load_onet_green_topics,
)
from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupations_data_processing import (
    process_green_gla_soc,
    process_green_timeshare_soc,
)
from dap_prinz_green_jobs.pipeline.green_measures.occupations.soc_map import SOCMapper

from dap_prinz_green_jobs import logger


def clean_job_title(job_title: str) -> str:
    """Cleans the job title

    :param job_title: A job title
    :type job_title: str

    :return: A cleaned job title
    :rtype: str

    """

    return job_title.lower()


def process_green_topics(green_topics):
    soc_mapper = SOCMapper(
        local=False,
        embeddings_output_dir="outputs/data/green_occupations/green_topic_matching/",
    )
    soc_mapper.load(save_embeds=True, job_titles=False)

    unique_us_occupations = [g for g in green_topics["Occupation"].tolist() if g]
    green_topic_soc = soc_mapper.get_soc(job_titles=unique_us_occupations)

    # US SOC <> UK 2020 SOC <> UK 2010 SOC
    green_topics_2_soc2020 = dict(zip(unique_us_occupations, green_topic_soc))
    green_topics["SOC_2020"] = green_topics["Occupation"].apply(
        lambda x: green_topics_2_soc2020[x][0][0] if green_topics_2_soc2020[x] else None
    )
    green_topics["SOC_2020_name"] = green_topics["Occupation"].apply(
        lambda x: green_topics_2_soc2020[x][1] if green_topics_2_soc2020[x] else None
    )
    green_topics["SOC_2010"] = green_topics["Occupation"].apply(
        lambda x: green_topics_2_soc2020[x][0][2] if green_topics_2_soc2020[x] else None
    )

    # Load the GLA mapping data from US SOC TO UK SOC 2010
    green_gla_data = pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/occupation_data/gla/Summary of green occupations (Nov 2021).xlsx",
        sheet_name="5. Mapping from ONET to UK SOC",
        skiprows=3,
        converters={"UK SOC2010 code": str},
    )

    green_gla_data["soc_code_name"] = green_gla_data.apply(
        lambda x: (x["UK SOC2010 code"], x["UK SOC2010 title"]), axis=1
    )
    gla_mapper = dict(
        zip(green_gla_data["O*NET Code"], green_gla_data["soc_code_name"])
    )

    # Add this to the green topics data
    green_topics["SOC_2010_GLA"] = green_topics["Code"].apply(
        lambda x: gla_mapper.get(x)[0] if gla_mapper.get(x) else None
    )
    green_topics["SOC_2010_name_GLA"] = green_topics["Code"].apply(
        lambda x: gla_mapper.get(x)[1] if gla_mapper.get(x) else None
    )

    # Use GLA if we don't find it
    # Of the 1354 ONET occupations, we find 2010 SOC for 1081 of them (80%)
    # Of the 1354 ONET occupations, GLA find 2010 SOC for 721 of them (53%)
    # We could recover 141 of our 273 not found ones by using GLA's
    green_topics["SOC_2010"] = green_topics.apply(
        lambda x: x["SOC_2010"] if x["SOC_2010"] else x["SOC_2010_GLA"], axis=1
    )
    green_topics["SOC_name"] = green_topics.apply(
        lambda x: x["SOC_2020_name"] if x["SOC_2020_name"] else x["SOC_2010_name_GLA"],
        axis=1,
    )

    green_topics["SOC_name"] = green_topics["SOC_name"].apply(
        lambda x: list(x)[0] if isinstance(x, set) else x
    )

    # We now need to duplicate rows where there are multipl SOC 2010 codes (each row was per unique 2020 code)
    green_topics = green_topics.explode("SOC_2010")

    return green_topics, green_topics_2_soc2020


class OccupationMeasures(object):
    """
    Class to extract occupation measures for a given job advert or list of job adverts.

    ----------
    Arguments
    ----------
    The arguments to this class are all for the use of SOCMapper - as such more information about them can be found in soc_map.py
    For most purposes they can be kept as their default values

    ----------
    Methods
    ----------
    get_measures(job_advert, job_title_key):
        for a given job advert (dict) or list of job adverts (list of dicts), extract the occupation-level green measures.
    precalculate_soc_mapper():
        Loading the SOC mapper (job titles to SOC) is more efficient if you run it on all unique job titles first, rather than one by one
    get_green_measure_for_job_title(self, job_title):
        Get the green measures for a single job title
    get_measures(job_adverts, job_title_key):
        Get the green measures for many job adverts (in dict format where the job title is given in the job_title_key key)

    ----------
    Usage
    ----------
    from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupations_measures_utils import OccupationMeasures

    om = OccupationMeasures()
    om.load()
    unique_job_titles = ["Data Scientist", "Nurse", "Key Stage 4 teacher", "Pharmacist", "Biologist"]
    job_title_2_match = om.precalculate_soc_mapper(unique_job_titles)
    om.get_green_measure_for_job_title("Data Scientist")
    >>> {'GREEN CATEGORY': 'Green New & Emerging', 'GREEN/NOT GREEN': 'Green', 'GREEN TIMESHARE': 12.5, 'GREEN TOPICS': 30, 'SOC': {'SOC_2020_EXT': '2433/02', 'SOC_2020': '2433', 'SOC_2010': '2425', 'name': ['Mathematicians ', 'Data scientists', 'Economists', 'Statisticians ']}}
    or
    om.get_measures(job_adverts= [{'description': 'We are looking for a sales ...', 'job_title': 'Data Scientist'}], job_title_key='job_title')
    """

    def load(
        self,
        local=False,
        embeddings_output_dir="outputs/data/green_occupations/soc_matching/",
        batch_size=500,
        match_top_n=10,
        sim_threshold=0.67,
        top_n_sim_threshold=0.5,
        minimum_n=3,
        minimum_prop=0.5,
        save_embeds=True,
    ):
        # Load the datasets
        green_gla_data = process_green_gla_soc(load_green_gla_soc())
        green_timeshares = process_green_timeshare_soc(load_green_timeshare_soc())
        green_topics = load_onet_green_topics()

        self.soc_mapper = SOCMapper(
            local=local,
            embeddings_output_dir=embeddings_output_dir,
            batch_size=batch_size,
            match_top_n=match_top_n,
            sim_threshold=sim_threshold,
            top_n_sim_threshold=top_n_sim_threshold,
            minimum_n=minimum_n,
            minimum_prop=minimum_prop,
        )
        self.soc_mapper.load(save_embeds=save_embeds)

        logger.info("Predict UK SOC for the occupations in the ONET green topics data")

        green_topics, self.green_topics_2_soc2020 = process_green_topics(green_topics)

        # The list of green topics per occupation in the ONET data e.g. {'9120': ['Land use planning', 'Green construction', 'Earth science',]..}
        onet_green_topics = (
            green_topics.groupby("SOC_2010")
            .agg({"Topic": lambda x: list(set(x)), "SOC_name": lambda x: list(set(x))})
            .reset_index()
        )

        onet_green_topics.rename(columns={"Topic": "ONET_green_topics"}, inplace=True)

        # Combine all the green measures per SOC 2010
        green_soc_data = pd.merge(
            green_gla_data,
            green_timeshares,
            how="outer",
            on="SOC_2010",
        )
        green_soc_data = pd.merge(
            green_soc_data,
            onet_green_topics,
            how="outer",
            on="SOC_2010",
        )

        self.soc_green_measures_dict = green_soc_data.set_index("SOC_2010")[
            [
                "SOC_name",
                "GLA_Green Category",
                "GLA_Green/Non-green",
                "timeshare_2019",
                "ONET_green_topics",
            ]
        ].T.to_dict()

    def precalculate_soc_mapper(
        self,
        unique_job_titles,
    ):
        """
        This just needs to be done once to calculate the SOCs for each unique job title in the dataset
        It's quicker to use soc_mapper with a bulk unique input, rather than use it one job title at a time

        Args:
            unique_job_titles (set): The job titles you want to find SOCs for

        Returns:
            dict: job title to SOC maps

        """

        soc_matches = self.soc_mapper.get_soc(job_titles=unique_job_titles)
        self.job_title_2_match = dict(zip(unique_job_titles, soc_matches))

        return self.job_title_2_match

    def get_green_measure_for_job_title(self, job_title, return_all_green_topics=False):
        """
        Get the green measures for a single job title

        Args:
            job_title (str): A job title
            return_all_green_topics (bool): Whether you want to return all the ONET green topics linked or just a count.
        Returns:
            dict: Green measures for this job title

        """

        soc_match = self.job_title_2_match.get(job_title, None)
        if soc_match:
            soc_info = {
                "SOC_2020_EXT": soc_match[0][0],
                "SOC_2020": soc_match[0][1],
                "SOC_2010": soc_match[0][2],
            }
            soc_2010 = soc_match[0][2]

            green_occ_measures = self.soc_green_measures_dict.get(soc_2010)
            if green_occ_measures:
                soc_info["name"] = green_occ_measures.get("SOC_name")
                if return_all_green_topics:
                    green_topics = green_occ_measures.get("ONET_green_topics")
                else:
                    green_topics = (
                        len(green_occ_measures.get("ONET_green_topics"))
                        if green_occ_measures.get("ONET_green_topics")
                        else 0
                    )

                return {
                    "GREEN CATEGORY": green_occ_measures.get("GLA_Green Category"),
                    "GREEN/NOT GREEN": green_occ_measures.get("GLA_Green/Non-green"),
                    "GREEN TIMESHARE": green_occ_measures.get("timeshare_2019"),
                    "GREEN TOPICS": green_topics,
                    "SOC": soc_info,
                }
            else:
                return {
                    "GREEN CATEGORY": None,
                    "GREEN/NOT GREEN": None,
                    "GREEN TIMESHARE": None,
                    "GREEN TOPICS": 0,
                    "SOC": None,
                }
        else:
            return {
                "GREEN CATEGORY": None,
                "GREEN/NOT GREEN": None,
                "GREEN TIMESHARE": None,
                "GREEN TOPICS": 0,
                "SOC": None,
            }

    def get_measures(self, job_adverts, job_title_key, return_all_green_topics=False):
        if type(job_adverts) == dict:
            job_adverts = [job_adverts]

        occ_green_measures_list = []
        for job in job_adverts:
            occ_green_measures_list.append(
                self.get_green_measure_for_job_title(
                    job[job_title_key], return_all_green_topics=return_all_green_topics
                )
            )

        return occ_green_measures_list
