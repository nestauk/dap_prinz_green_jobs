"""
Functions and variables to map company descriptions
    to SIC codes.
"""
from typing import List, Dict
from dap_prinz_green_jobs.getters.industry_getters import load_sic

import numpy as np
import re

sic_data = load_sic()
sic_to_section = {
    str(k).strip(): v.strip()
    for k, v in dict(
        zip(sic_data["Most disaggregated level"], sic_data["SECTION"])
    ).items()
}
# Found using the most common words in CH data
company_stopwords = set(
    [
        "limited",
        "inc",
        "llc",
        "ltd",
        "apps",
        "co",
        "the",
        "services",
        "management",
        "company",
        "uk",
        "c",
        "llp",
        "lp",
        "international",
        "group",
        "cic",
        "plc",
    ]
)


def clean_sic(sic_name: str) -> str:
    """Cleans the SIC code.

    Args:
        sic_name (str): The SIC code

    Returns:
        str: The cleaned SIC code
    """
    if sic_name is None:
        return None

    if not isinstance(sic_name, str):
        sic_name = str(sic_name)

    if sic_name:
        sic = str(sic_name.split(" - ")[0])
        if len(sic) == 4:
            return "0" + sic
        else:
            return sic
    else:
        return None


def clean_company_name(
    name: str,
    word_mapper: dict = {},
    company_stop_words: set = company_stopwords,
) -> str:
    """Clean the company name so it can be matched across datasets

    There is lots of different ways to write company names, so this normalises
    across datasets for matching. e.g. "Apple ltd." and "apple limited"

    :param name: A company name
    :type: str
    :param word_mapper: A dict of words to replace with others (e.g. {"ltd": "limited"})
    :type: dict
    :param company_stop_words: words to be removed from the name
    :type: set
    :return: A cleaned company name
    :rtype: str
    """

    if name:
        name = str(name)
        name = re.sub(r"[^\w\s]", "", name)
        name = " ".join(name.split())  # sort out double spaces and trailing spaces
        name = name.lower()

        name_words = name.split()
        words = [
            word_mapper.get(word, word)
            for word in name_words
            if word not in company_stop_words
        ]

        name = " ".join(words)

        if len(name) > 2:
            return name
        else:
            return None
    else:
        return None


# We can modify this slightly when we use a company name to SIC mapper
# instead of companies house data
def get_ch_sic(company_name: str, ojo_companies_house_dict: Dict) -> str:
    """
    Pick the first 5 digit SIC for each cleaned name
        using the Companies House data. Assumes that companies house
        SICs are ordered in terms of importance.

    :type companies_house_cleaned_in_ojo_dict: dict

    :param cleaned_name: The cleaned company name
    :type cleaned_name: str

    :return: A SIC or None
    :rtype: str or None

    """
    cleaned_name = clean_company_name(company_name)

    companies_house_data = ojo_companies_house_dict.get(cleaned_name)
    if companies_house_data:
        sic_options = [
            c["SICCode.SicText_1"]
            for c in companies_house_data
            if c["SICCode.SicText_1"]
        ]
        # return the first sic code
        sic_code = clean_sic(sic_options[0])
        return sic_code
    else:
        return None


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
