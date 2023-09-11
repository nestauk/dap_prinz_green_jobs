"""
Functions and variables to map company descriptions
    to SIC codes.
"""
from typing import List, Dict, Union
import pandas as pd
import ast
import itertools


def convert_indx_to_sic(
    top_k_indices: List[int], sic_company_descriptions: List[Dict[str, str]]
) -> List[str]:
    """Convert indx to SIC codes.

    Args:
        top_k_indices (List[int]): List of indices
        sic_company_descriptions (List[Dict[str, str]]): List of SIC code description dictionaries

    Returns:
        List[str]: List of SIC codes
    """
    top_sic_codes = [sic_company_descriptions[i]["sic code"] for i in top_k_indices]

    return [str(i[0]) for i in top_sic_codes]


def convert_faiss_distance_to_score(faiss_distance: float) -> float:
    """Converts a faiss distance to a
    similarity score between 0 and 1.

    Args:
        faiss_distance (float): Distance

    Returns:
        float: Similarity score
    """
    return 1 / (1 + faiss_distance)


def add_sic_section(
    input_list: List[str],
    sic_to_section: Dict[str, str],
) -> List[str]:
    """Appends the SIC section to the SIC code.

    Args:
        input_list (List[str]): List of SIC codes
        sic_to_section (Dict[str, str]): Dictionary mapping SIC codes to SIC sections

    Returns:
        List[str]: List of SIC codes with the section appended
    """
    sic_code_section = []
    for sic_code in input_list:
        sic_section = sic_to_section.get(sic_code, None)
        if sic_section:
            sic_code_section.append(f"{sic_section.strip()}{sic_code.strip()}")
        else:
            sic_code_section.append(sic_code)

    return sic_code_section


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

    # Find the minimum length of the two strings
    min_len = min(len(str1), len(str2))

    # Iterate through the characters of both strings up to the minimum length
    for i in range(min_len):
        if str1[i] == str2[i]:
            common_prefix.append(str1[i])
        else:
            break

    return common_prefix


def find_majority_sic(input_list: List[str]) -> Union[List[str], List[None]]:
    """Finds the majority SIC code from a list of SIC codes.

    Args:
        input_list (List[str]): The list of SIC codes

    Returns:
        Union[List[str], List[None]]: The majority SIC code(s)
    """
    # add SIC sections to the SIC codes
    input_list_section = add_sic_section(input_list)

    # generate all possible unique combinations of the SIC codes
    # of length 2
    sic_combos = list(set(list(itertools.combinations(input_list_section, 2))))

    # identify the longest common prefix between each combination
    sic_dists = [longest_common_prefix(sic1, sic2) for sic1, sic2 in sic_combos]

    return ["".join(sic_dist) for sic_dist in sic_dists if sic_dist != []]
