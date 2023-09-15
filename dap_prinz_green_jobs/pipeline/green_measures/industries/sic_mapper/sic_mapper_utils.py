"""
Functions and variables to map company descriptions
    to SIC codes.
"""
from typing import List, Dict
from dap_prinz_green_jobs.getters.industry_getters import load_sic

import numpy as np

sic_data = load_sic()
sic_to_section = {
    str(k).strip(): v.strip()
    for k, v in dict(
        zip(sic_data["Most disaggregated level"], sic_data["SECTION"])
    ).items()
}


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
        if len(sic_code) == 1:
            return f"0{sic_code}"
        elif len(sic_code) == 4:
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


def add_sic_section(
    sic_codes: List[str], sic_section_dict: Dict[str, str] = sic_to_section
) -> List[str]:
    """Adds the SIC section to the SIC code.

    Args:
        sic_codes (List[str, str]): List of SIC codes
        sic_section (Dict[str,str]): Dictionary of SIC sections

    Returns:
        List[str]: List of SIC codes with sections
    """

    if not isinstance(sic_codes, list):
        sic_codes = [sic_codes]

    sic_code_section = []
    for sic_code in sic_codes:
        if sic_code.isdigit():
            sic_section = sic_section_dict.get(sic_code)
            if sic_section:
                sic_code_section.append(f"{str(sic_section).strip()}{sic_code}")
            else:
                sic_code_section.append(str(sic_code))
        else:
            sic_code_section.append(str(sic_code))

    return sic_codes


def find_majority_sic(input_list: List[str], length: int) -> Dict[str, int]:
    """Finds the majority SIC code.

    Args:
        input_list (List[str]): List of SIC codes
        length (int): Length of SIC code to count

    Returns:
        Dict[str, int]: Dictionary of SIC codes and weighted counts
    """
    if not input_list or length <= 0:
        return {}

    input_sics_sections = add_sic_section(input_list)

    subelement_count = {}

    for sic_code in input_sics_sections:
        if len(sic_code) >= length:
            substring = sic_code[:length]
            if substring in subelement_count:
                subelement_count[substring] += 1
            else:
                subelement_count[substring] = 1

    sorted_substring_count = {
        k: v
        for k, v in sorted(
            subelement_count.items(), key=lambda item: item[1], reverse=True
        )
    }

    return {k: v for k, v in sorted_substring_count.items() if v > 1}


def calculate_average_distance(
    top_sic_code: str, top_sic_codes: List[str], top_sic_distances: List[int]
) -> int:
    """Returns the average distance of the top k SIC codes to the input SIC code.

    Args:
        top_sic_code (str): Top SIC code
        top_sic_codes (List[str]): List of top k SIC codes
        top_sic_distances (List[int]): List of top k distances

    Returns:
        int: Average top SIC code distance
    """
    top_sic_probs = [
        top_sic_distances[i]
        for i, sic_code in enumerate(top_sic_codes)
        if sic_code.startswith(top_sic_code)
    ]

    return round(np.mean(top_sic_probs), 2)
