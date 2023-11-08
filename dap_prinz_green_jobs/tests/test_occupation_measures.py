import pytest
from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupations_measures_utils import (
    OccupationMeasures,
)
from dap_prinz_green_jobs.pipeline.green_measures.occupations.soc_map import SOCMapper
from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupations_data_processing import (
    job_title_cleaner,
)


def test_occupation_measures():
    om = OccupationMeasures()
    om.soc_green_measures_dict = {
        "2114": {"timeshare_2019": 12.9},
        "2231": {"timeshare_2019": 0.0},
        "2213": {"timeshare_2019": 0.0},
        "2112": {"timeshare_2019": 15.6},
    }

    om.job_title_2_match = {
        "Data Scientist": (("2433/02", "2433", "2114"), "data scientist"),
        "Nurse": (("2237/00", "2237", "2231"), "nurse"),
    }

    assert (
        om.get_green_measure_for_job_title("Data Scientist")["GREEN TIMESHARE"] == 12.9
    )
    assert (
        om.get_green_measure_for_job_title("Data Scientist")["SOC"]["SOC_2020_EXT"]
        == "2433/02"
    )
    assert (
        om.get_green_measure_for_job_title("Data Scientist")["SOC"]["SOC_2020"]
        == "2433"
    )
    assert (
        om.get_green_measure_for_job_title("Data Scientist")["SOC"]["SOC_2010"]
        == "2114"
    )
    assert om.get_green_measure_for_job_title("Zoologist")["GREEN TIMESHARE"] == None

    assert (
        len(
            om.get_measures(
                job_adverts=[
                    {"job_title": "Data Scientist"},
                    {"job_title": "Nurse"},
                    {"job_title": "Zoologist"},
                ],
                job_title_key="job_title",
            )
        )
        == 3
    )


def test_job_title_cleaner():
    assert job_title_cleaner("Data Scientist - London") == "Data Scientist"
    assert (
        job_title_cleaner("Data Scientist - Part-time - London")
        == "Data Scientist - Part-time"
    )
    assert job_title_cleaner("Data Scientist - 16hrs") == "Data Scientist"
    assert job_title_cleaner("Data Scientist - &#163;1000 bonus") == "Data Scientist"
    assert job_title_cleaner("£30k Data Scientist") == "£30k Data Scientist"
    assert job_title_cleaner("Data Scientist £30k") == "Data Scientist"
    assert job_title_cleaner("Remote - Data Scientist") == "Remote - Data Scientist"
