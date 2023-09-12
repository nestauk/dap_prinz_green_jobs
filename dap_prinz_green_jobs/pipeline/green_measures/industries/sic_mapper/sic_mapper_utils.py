"""
Functions and variables to map company descriptions
    to SIC codes.
"""
from typing import List, Dict, Union
import itertools


def clean_sic_code(sic_code: str) -> str:
    """Cleans a SIC code.

    Args:
        sic_code (str): SIC code to clean.

    Returns:
        str: clean SIC code
    """
    if not isinstance(sic_code, str):
        sic_code = str(sic_code)

    if sic_code.isdigit():
        if len(sic_code) == 4:
            return f"{sic_code}0"
        else:
            return sic_code
    else:
        return sic_code


def convert_indx_to_sic(
    top_k_indices: List[int], sic_company_descriptions: List[Dict[str, str]]
) -> List[str]:
    """Convert indx to cleaned SIC codes.

    Args:
        top_k_indices (List[int]): List of indices
        sic_company_descriptions (List[Dict[str, str]]): List of SIC code description dictionaries

    Returns:
        List[str]: List of SIC codes
    """
    top_sic_codes = [sic_company_descriptions[i]["sic_code"] for i in top_k_indices]

    return [str(i[0]).strip() for i in top_sic_codes]


def convert_faiss_distance_to_score(faiss_distance: float) -> float:
    """Converts a faiss distance to a
    similarity score between 0 and 1.

    Args:
        faiss_distance (float): Distance

    Returns:
        float: Similarity score
    """
    return 1 / (1 + faiss_distance)


def longest_common_prefix(str1: str, str2: str) -> str:
    """Finds the longest common prefix between two strings.

    Args:
        str1 (str): the first string
        str2 (str): the second string

    Returns:
        str: the longest common prefix between
            the two strings
    """
    common_prefix = []

    if not isinstance(str1, str) or not isinstance(str2, str):
        str1, str2 = str(str1), str(str2)

    # Find the minimum length of the two strings
    min_len = min(len(str1), len(str2))

    # Iterate through the characters of both strings up to the minimum length
    for i in range(min_len):
        if str1[i] == str2[i]:
            common_prefix.append(str1[i])
        else:
            break

    return common_prefix


# triple check that the majority sic codes are ordered i.e. closest first
def find_majority_sic(input_sics: List[str]) -> Union[List[str], List[None]]:
    """Finds the majority SIC codes from a list of SIC codes.

    Args:
        input_sics (List[str]): The list of SIC codes

    Returns:
        Union[List[str], List[None]]: The majority SIC codes
    """
    # generate all possible unique combinations of the SIC codes
    # of length 2
    sic_combos = list(set(list(itertools.combinations(input_sics, 2))))

    # identify the longest common prefix between each combination
    sic_dists = [longest_common_prefix(sic1, sic2) for sic1, sic2 in sic_combos]

    # get the longest common prefix
    dist_len = 0
    for dist in sic_dists:
        if len(dist) > dist_len:
            dist_len = len(dist)

    top_sic_codes = ["".join(sic) for sic in sic_dists if len(sic) == dist_len]

    # if its greater than 0
    if len(top_sic_codes) > 0:
        top_sic_code = clean_sic_code(top_sic_codes[0])  # assuming order
        return top_sic_code
    else:
        return None
