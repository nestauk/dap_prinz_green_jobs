from typing import List, Dict, Tuple
from itertools import islice
from ojd_daps_skills.pipeline.extract_skills.extract_skills import ExtractSkills
import numpy as np
import pandas as pd
from tqdm import tqdm


def format_skills(skill_label: List[str]) -> List[Dict[str, list]]:
    """Format extracted skills into a dictionary with the following keys:
    EXPERIENCE, SKILL, MULTISKILL

    Args:
        skill_label (List[str]): List of extracted skills

    Returns:
        List[Dict[str, list]]: Formatted list of a dictionary of extracted skills
    """
    if type(skill_label) == list:
        return [{"SKILL": [], "MULTISKILL": skill_label, "EXPERIENCE": []}]
    else:
        return skill_label


def get_green_skill_matches(
    extracted_skill_list: List[str],
    similarities: np.array,
    green_skills_taxonomy: pd.DataFrame(),
    skill_threshold: float = 0.7,
) -> List[Tuple[str, Tuple[str, int]]]:
    """Get green skill matches for a list of extracted skills - use this
        in extract_green_measures flow instead of get_green_skill_measures

        NOTE: this is because speeds up skills mapping considerably
        and because the esco green taxonomy is not hierarchical so we are simply
        matching the extracted skills to the green taxonomy based on a minimum
        threshold cosine similarity.

    Args:
        extracted_skill_list (List[str]): List of extracted skills

    Returns:
        List[Tuple[str, Tuple[str, int]]]: List of tuples with the extracted
            skill; the mapped green skill and a green skill id
    """
    skill_top_green_skills = []
    for skill_ix, skill in tqdm(enumerate(extracted_skill_list)):
        top_skill_matches = []
        for green_skill_ix in np.flip(np.argsort(similarities[skill_ix]))[0:1]:
            if similarities[skill_ix][0] > skill_threshold:
                green_skill = green_skills_taxonomy.iloc[
                    [green_skill_ix]
                ].description.values[0]
                green_skill_id = skill_ix
            else:
                green_skill = ""
                green_skill_id = None
            top_skill_matches.append((skill, (green_skill, green_skill_id)))
        skill_top_green_skills.extend(top_skill_matches)

    return skill_top_green_skills


def get_green_skill_measures(
    es: ExtractSkills,
    raw_skills,
    skill_hashes: Dict[int, str],
    job_skills: Dict[str, Dict[str, int]],
    skill_threshold: int = 0.5,
) -> List[dict]:
    """Extract green skills for job adverts.

    Args:
        es (ExtractSkills): instantiated ExtractSkills class
        skill_hashes (Dict[int, str]): Dictionary of skill hashes and skill names
        job_skills (Dict[str, Dict[str, int]]): dictionary of ids and extracted raw skills
        skill_threshold (int, optional): skill semantic similarity. Defaults to 0.5.

    Returns:
        List[dict]: list of dictionaries of green skills
    """

    # to get the output with the top ten closest skills
    mapped_skills = es.skill_mapper.map_skills(
        es.taxonomy_skills,
        skill_hashes,
        es.taxonomy_info.get("num_hier_levels"),
        es.taxonomy_info.get("skill_type_dict"),
    )

    matched_skills = []
    for i, (_, skill_info) in enumerate(job_skills.items()):
        job_skill_hashes = skill_info["skill_hashes"]
        job_skill_info = [
            sk for sk in mapped_skills if sk["ojo_skill_id"] in job_skill_hashes
        ]
        matched_skills_formatted = []
        for job_skill in job_skill_info:
            if job_skill["top_tax_skills"][0][2] > skill_threshold:
                matched_skills_formatted.append(
                    (
                        job_skill["ojo_ner_skill"],
                        (
                            job_skill["top_tax_skills"][0][0],
                            job_skill["top_tax_skills"][0][1],
                        ),
                    )
                )
            else:
                matched_skills_formatted.append(
                    (
                        job_skill["ojo_ner_skill"],
                        ("", 0),
                    )
                )
        matched_skills.append(
            {
                "SKILL": matched_skills_formatted,
                "EXPERIENCE": raw_skills[i]["EXPERIENCE"],
            }
        )

    return matched_skills
