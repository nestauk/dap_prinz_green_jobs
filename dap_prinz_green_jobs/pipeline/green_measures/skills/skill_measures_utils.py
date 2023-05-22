from typing import List, Dict
from itertools import islice
from ojd_daps_skills.pipeline.extract_skills.extract_skills import ExtractSkills


def chunks(data_dict: dict, chunk_size: int = 100):
    """Chunks data dictionary into batches of a specified chunk_size.

    Args:
        data_dict: dictionary of job adverts where key is job id
            and value is a list of skills
        chunk_size (int, optional): chunk size. Defaults to 100.

    Yields:
        _type_: job advert chunks
    """
    it = iter(data_dict)
    for i in range(0, len(data_dict), chunk_size):
        yield {k: data_dict[k] for k in islice(it, chunk_size)}


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
