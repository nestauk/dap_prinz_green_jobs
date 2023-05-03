from typing import List, Dict
from itertools import islice


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


def format_skills(skill_label: List[str]) -> Dict[str, List[str]]:
    """Format extracted skills into a dictionary with the following keys:
    EXPERIENCE, SKILL, MULTISKILL

    Args:
        skill_label (List[str]): List of extracted skills

    Returns:
        Dict[str, List[str]]: Formatted dictionary of extracted skills
    """
    if type(skill_label) == list:
        return {"EXPERIENCE": [], "SKILL": skill_label, "MULTISKILL": []}
    else:
        return skill_label
