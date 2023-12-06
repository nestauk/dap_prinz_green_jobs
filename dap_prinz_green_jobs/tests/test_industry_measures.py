import pytest

from dap_prinz_green_jobs.pipeline.green_measures.industries.industries_measures_utils import (
    get_ghg_sic,
    clean_total_emissions_dict,
    clean_unit_emissions_dict,
)

from dap_prinz_green_jobs.pipeline.green_measures.industries.sic_mapper.sic_mapper_utils import (
    clean_sic,
)

import os

job_ads = [
    {
        "id": 1,
        "job_text": "We are a technology company that makes phones. We are hiring a software engineer.",
    },
    {
        "id": 2,
        "job_text": "We are a major jewellery brand. We are hiring a sales assistant.",
    },
    {
        "id": 3,
        "job_text": "Our business is selling cars. We need a car salesman.",
    },
    {"id": 4, "company_name": "fake_company_name", "job_text": "Nothing helpful here."},
    {
        "id": "5",
        "job_text": "At Menzies Distribution, we're looking for a driver to join.",
    },
]


def test_clean_sic():
    assert clean_sic('82911 - "Activities of collection agencies"') == "82911"
    assert clean_sic('8121 - "General cleaning of buildings"') == "08121"
    assert clean_sic(None) == None
    assert clean_sic("") == None


def test_get_ghg_sic():
    ghg_emissions_dict = {"01": 10, "82": 20, "254": 30, "2351": 40}
    assert get_ghg_sic("01121", ghg_emissions_dict) == 10
    assert get_ghg_sic("82911", ghg_emissions_dict) == 20
    assert get_ghg_sic("25420", ghg_emissions_dict) == 30
    assert get_ghg_sic("23513", ghg_emissions_dict) == 40
    assert get_ghg_sic("01", ghg_emissions_dict) == 10
    assert get_ghg_sic(None, ghg_emissions_dict) == None
    assert get_ghg_sic("1234", ghg_emissions_dict) == None


def test_clean_total_emissions_dict():
    assert clean_total_emissions_dict({1: 10}) == {"01": 10}
    assert clean_total_emissions_dict({82: 10}) == {"82": 10}
    assert clean_total_emissions_dict({25.4: 10}) == {"254": 10}


def test_clean_unit_emissions_dict():
    assert clean_unit_emissions_dict({1: 10}) == {"01": 10}
    assert clean_unit_emissions_dict({82: 10}) == {"82": 10}
    assert clean_unit_emissions_dict({25.4: 10}) == {"254": 10}
    assert clean_unit_emissions_dict({"10.2-3": 10}) == {"102": 10, "103": 10}


IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"


@pytest.mark.skipif(IN_GITHUB_ACTIONS, reason="Test doesn't work in Github Actions.")
def test_industry_measures():
    from dap_prinz_green_jobs.pipeline.green_measures.industries.industries_measures import (
        IndustryMeasures,
    )

    im = IndustryMeasures()
    im.load()

    industry_measures = im.get_measures(job_ads)

    # assert all keys in indsutries measures are string
    assert len(industry_measures) == len(job_ads)

    assert industry_measures["5"]["SIC_method"] == "hard coded sic"
    assert industry_measures[1]["SIC_method"] == "closest distance"
    assert industry_measures[4]["SIC"] == None
    assert industry_measures[4]["company_description"] == ""

    assert industry_measures[1]["SIC"] == "47421"
    assert industry_measures[2]["SIC"] == "32130"

    assert (
        industry_measures[2]["SIC_name"]
        == "Manufacture of imitation jewellery and related articles"
    )
    assert industry_measures[2]["INDUSTRY PROP HOURS GREEN TASKS"] == 12.1

    assert len(industry_measures[1].keys()) == 12
    assert len(industry_measures[3].keys()) == 12

    assert type(industry_measures[2]["INDUSTRY PROP HOURS GREEN TASKS"]) == float
    assert industry_measures[4]["SIC"] == None
    assert type(industry_measures["5"]["SIC"]) == str
