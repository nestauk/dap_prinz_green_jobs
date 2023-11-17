"""
Functions and utils to deal with duplication in job adverts and to sample OJO data.

get_deduplicated_job_adverts takes the job adverts (only job id and date columns are needed)
and chunks up the dates and then removes duplicates from each chunk, before recombining.

"""

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from hashlib import md5

from dap_prinz_green_jobs import logger

import pandas as pd
from typing import List, Union

desired_sample_size = 1000000
random_seed = 42
production = "true"


def short_hash(text: str) -> int:
    """Generate a unique short hash for this string - from ojd_daps"""
    hx_code = md5(text.encode()).hexdigest()
    int_code = int(hx_code, 16)
    short_code = str(int_code)[:16]
    return int(short_code)


def get_date_ranges(
    start: str, end: str, num_units: int = 7, unit_type: str = "days"
) -> list:
    """
    Chunk up a range of dates into user specified units of time.
    Inputs:
        start, end: strings of a date in the form %Y-%m-%d (e.g. '2021-05-31')
        num_units: time period of chunks, e.g. if num_units=3 and unit_type="days" then this is 3 days
        unit_type: ["days", "weeks", "months"]
    Outputs:
        A list of the start and end dates to each chunk
        The last chunk may be partial.
    """
    start = datetime.strptime(start, "%Y-%m-%d")
    end = datetime.strptime(end, "%Y-%m-%d")

    if unit_type == "days":
        time_addition = timedelta(days=num_units)
    elif unit_type == "weeks":
        time_addition = timedelta(weeks=num_units)
    elif unit_type == "months":
        time_addition = relativedelta(months=num_units)
    else:
        print('unit_type not recognised - use ["days", "weeks", "months"]')

    chunk_start = start
    chunk_end = chunk_start + time_addition
    list_ranges = []
    while chunk_end < end:
        list_ranges.append((chunk_start, chunk_end))
        chunk_start += time_addition
        chunk_end = chunk_start + time_addition
    list_ranges.append((chunk_start, end))
    return list_ranges


def create_date_chunks_mapper(
    first_date: str, last_date: str, num_units: int = 7, unit_type: str = "days"
) -> dict:
    """
    Gets a dict of all dates in the range and which start date chunk they map to.
    e.g. if num_units=2 and unit_type="days"
    {
    '2021-06-01': '2021-06-01',
    '2021-06-02': '2021-06-01',
    '2021-06-03': '2021-06-03',
    '2021-06-04': '2021-06-03'
    }
    """

    date_ranges_list = get_date_ranges(
        first_date, last_date, num_units, unit_type=unit_type
    )

    date_chunk_mapper = {}
    for start, end in date_ranges_list:
        curr_date = start
        while curr_date < end:
            date_chunk_mapper[curr_date.strftime("%Y-%m-%d")] = end.strftime("%Y-%m-%d")
            curr_date += timedelta(days=1)
        date_chunk_mapper[curr_date.strftime("%Y-%m-%d")] = end.strftime("%Y-%m-%d")
    return date_chunk_mapper


def check_span(num_units, unit_type):
    warning_message = "This is designed for use with spans < 8 weeks"

    if unit_type == "days":
        if num_units >= 56:
            logger.warning(warning_message)
    elif unit_type == "weeks":
        if num_units >= 8:
            logger.warning(warning_message)
    elif unit_type == "months":
        if num_units >= 1:
            logger.warning(warning_message)


def get_deduplicated_job_adverts(
    job_adverts: pd.DataFrame(),
    num_units: int = 7,
    unit_type: str = "days",
    id_col: str = "id",
    date_col: str = "created",
    job_loc_col: str = "job_location_raw",
    description_hash: str = "description_hash",
):
    """
    Find the job ids of the deduplicated job adverts based on whether they had any
    duplicates found in job stock chunks (date spans).

    Input:
        job_adverts: pandas DataFrame with job_id, date, job_loc_col and description_hash columns
            The date column must be in the form %Y-%m-%d e.g. '2021-05-31'
        num_units: time period of chunks, e.g. if num_units=3 and unit_type="days" then this is 3 days
        unit_type: ["days", "weeks", "months"]

    Output:
        no_duplicates: pandas DataFrame with job_id and which date
            chunk it belong to columns. Duplicates have been removed
    """

    check_span(num_units, unit_type)

    date_chunk_mapper = create_date_chunks_mapper(
        job_adverts[date_col].min(),
        job_adverts[date_col].max(),
        num_units=num_units,
        unit_type=unit_type,
    )

    job_adverts["end_date_chunk"] = job_adverts[date_col].map(date_chunk_mapper)

    # Randomly sort and drop duplicates
    no_duplicates = job_adverts.sample(frac=1, random_state=42).drop_duplicates(
        ["end_date_chunk", job_loc_col, description_hash]
    )

    return no_duplicates[["id", "end_date_chunk"]]


def get_soc4_codes(soc_codes: Union[List[tuple], None]) -> Union[List[str], None]:
    """Get the SOC4 codes from extracted SOC codes.

    Args:
        soc_codes (Union[List[tuple], None]): Extracted SOC codes.

    Returns:
        Union[List[str], None]: SOC4 codes.
    """
    soc4_codes = []
    for soc_info in soc_codes:
        if isinstance(soc_info, tuple):
            if len(soc_info[0]) == 3:
                soc_code = soc_info[0][1]
            else:
                soc_code = soc_info[0]
        else:
            soc_code = None
        soc4_codes.append(soc_code)
    return soc4_codes
