"""
Utils for occuational green measures
"""

import pandas as pd

from typing import List, Union

from dap_prinz_green_jobs.getters.occupation_getters import (
    load_green_gla_soc,
    load_green_timeshare_soc,
)
from dap_prinz_green_jobs.utils.occupations_data_processing import (
    process_green_gla_soc,
    process_green_timeshare_soc,
)


def clean_job_title(job_title: str) -> str:
    """Cleans the job title

    :param job_title: A job title
    :type job_title: str

    :return: A cleaned job title
    :rtype: str

    """

    return job_title.lower()


def get_green_soc_measures() -> pd.DataFrame():
    """
    Load and merge the green soc datasets on SOC 2010 into one dataset
    """
    green_gla_data = process_green_gla_soc(load_green_gla_soc())
    green_timeshares = process_green_timeshare_soc(load_green_timeshare_soc())

    green_soc_data = pd.merge(
        green_gla_data,
        green_timeshares,
        how="outer",
        on="SOC_2010",
    )

    return green_soc_data
