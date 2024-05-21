import pytest

from dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils import (
    SkillMeasures,
    window_split,
    split_up_skill_entities,
)


def test_split_up_skill_entities():
    assert len(split_up_skill_entities(" ")) == 0
    assert (
        len(
            split_up_skill_entities(
                "small entity", max_entity_size=10, window_overlap=5
            )
        )
        == 1
    )

    entity = "word1 word2 word3 word4 word5 word6"
    max_entity_size = 3
    window_overlap = 2
    split_result = split_up_skill_entities(
        entity, max_entity_size=max_entity_size, window_overlap=window_overlap
    )

    assert len(split_result) == 4
    assert len(split_result[0].split()) == max_entity_size
    assert split_result[0] == "word1 word2 word3"
    assert split_result[1] == "word2 word3 word4"

    entity = "word1 word2; word3 word4; word5 word6"
    max_entity_size = 3
    window_overlap = 2
    split_result = split_up_skill_entities(
        entity, max_entity_size=max_entity_size, window_overlap=window_overlap
    )

    assert len(split_result) == 3


def test_skills_measures():
    sm = SkillMeasures(
        config_name="extract_green_skills_esco", initialise_gs_classifier=False
    )

    ents_per_job = {
        "123": [(["communication"], "SKILL"), (["Excel"], "SKILL")],
        "456": [(["Heat pump installation"], "SKILL")],
        "789": [],
        "abc": [
            (
                [
                    "Heat pump installation",
                    "installation and boiler",
                    "boiler upgrade skills",
                ],
                "MULTISKILL",
            )
        ],
        "def": [(["Skill not found in all_extracted_green_skills_dict"], "SKILL")],
    }
    all_extracted_green_skills_dict = {
        "communication": ["not-green", 0.9, []],
        "Excel": ["not-green", 0.95, []],
        "verbal communication": ["not-green", 0.85, []],
        "Heat pump installation": ["green", 0.9, []],
        "installation and boiler": ["not-green", 0.65, []],
        "boiler upgrade skills": ["not-green", 0.85, []],
    }
    job_benefits_dict = {"123": [], "456": ["pension"]}

    prop_green_skills = sm.calculate_measures(
        ents_per_job,
        all_extracted_green_skills_dict,
        job_benefits_dict,
    )

    assert set(prop_green_skills.keys()) == set(ents_per_job.keys())
    assert prop_green_skills["789"]["PROP_GREEN"] == 0
    assert prop_green_skills["789"]["ENTS"] == None
    assert (
        prop_green_skills["abc"]["PROP_GREEN"]
        == len(prop_green_skills["abc"]["GREEN_ENTS"])
        / prop_green_skills["abc"]["NUM_SPLIT_ENTS"]
    )
    assert prop_green_skills["456"]["BENEFITS"] == ["pension"]
