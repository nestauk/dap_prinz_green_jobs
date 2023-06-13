import pytest

from dap_prinz_green_jobs.pipeline.green_measures.industries.industries_measures_utils import (
    IndustryMeasures,
    clean_sic,
    clean_company_name,
    get_ghg_sic,
    clean_total_emissions_dict,
    clean_unit_emissions_dict,
)


def test_clean_sic():
    assert clean_sic('82911 - "Activities of collection agencies"') == "82911"
    assert clean_sic('8121 - "General cleaning of buildings"') == "08121"
    assert clean_sic(None) == None
    assert clean_sic("") == None


def test_clean_company_name():
    assert clean_company_name("Apple ltd.") == "apple"
    assert clean_company_name("") == None
    assert clean_company_name(None) == None


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


def test_industry_measures():
    im = IndustryMeasures()

    # Just use a very small replicated part of the data so we can see it's working as we'd like
    im.ojo_companies_house_dict = {
        "boots": [
            {
                "CompanyName": "BOOTS INTERNATIONAL LIMITED",
                "SICCode.SicText_1": "46450 - Wholesale of perfume and cosmetics",
            }
        ],
        "j sainsbury": [
            {
                "CompanyName": "J SAINSBURY PLC",
                "SICCode.SicText_1": "47110 - Retail sale in non-specialised stores with food, beverages or tobacco predominating",
            }
        ],
        "global british petroleum": [
            {
                "CompanyName": "GLOBAL BRITISH PETROLEUM LTD",
                "SICCode.SicText_1": "09100 - Support activities for petroleum and natural gas extraction",
                "SICCode.SicText_2": "46120 - Agents involved in the sale of fuels, ores, metals and industrial chemicals",
                "SICCode.SicText_3": "46690 - Wholesale of other machinery and equipment",
                "SICCode.SicText_4": "46750 - Wholesale of chemical products",
            }
        ],
    }

    im.ghg_emissions_dict = {"464": 6299.6, "09": 28.2}
    im.sic_section_2_prop_hours = {"G": 9.4}

    assert im.get_green_measure_for_company("Boots")["SIC"] == "46450"
    assert (
        im.get_green_measure_for_company("Boots")["INDUSTRY TOTAL GHG EMISSIONS"]
        == 6299.6
    )
    assert (
        im.get_green_measure_for_company("Boots")["INDUSTRY PROP HOURS GREEN TASKS"]
        == 9.4
    )
    assert (
        im.get_green_measure_for_company("Global British Petroleum")["SIC"] == "09100"
    )
    assert (
        im.get_green_measure_for_company("Global British Petroleum")[
            "INDUSTRY TOTAL GHG EMISSIONS"
        ]
        == 28.2
    )
    assert (
        im.get_green_measure_for_company("Global British Petroleum")[
            "INDUSTRY PROP HOURS GREEN TASKS"
        ]
        == None
    )
