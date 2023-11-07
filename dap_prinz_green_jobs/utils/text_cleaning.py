"""
Functions to minimally clean job advertisements.
"""
from toolz import pipe
import re
from typing import List

from hashlib import md5

# Pattern for fixing a missing space between enumerations, for
# split_sentences()
compiled_missing_space_pattern = re.compile("([a-z])([A-Z])")
# Characters outside these rules will be padded, for pad_punctuation()
compiled_nonalphabet_nonnumeric_pattern = re.compile(r"([^a-zA-Z0-9] )")

# The list of camel cases which should be kept in
exception_camelcases = [
    "JavaScript",
    "WordPress",
    "PowerPoint",
    "CloudFormation",
    "CommVault",
    "InDesign",
    "GitHub",
    "GitLab",
    "DevOps",
    "QuickBooks",
    "TypeScript",
    "XenDesktop",
    "DevSecOps",
    "CircleCi",
    "LeDeR",
    "CeMap",
    "MavenAutomation",
    "SaaS",
    "iOS",
    "MySQL",
    "MongoDB",
    "NoSQL",
    "GraphQL",
    "VoIP",
    "PhD",
    "HyperV",
    "PaaS",
    "ArgoCD",
    "WinCC",
    "AutoCAD",
]

# Any trailing chars that match these are removed
trim_chars = [" ", ".", ",", ";", ":", "\xa0"]


def detect_camelcase(text):
    """
    Splits a word written in camel-case into separate sentences. This fixes a case
    when the last word of a sentence in not seperated from the capitalised word of
    the next sentence. This tends to occur with enumerations.
    For example, the string "skillsBe" will be converted to "skills. Be"
    Some camelcases are allowed though - these are found and replaced. e.g. JavaScript
    Note that the present solution doesn't catch all such cases (e.g. "UKSkills")
    Reference: https://stackoverflow.com/questions/1097901/regular-expression-split-string-by-capital-letter-but-ignore-tla
    """
    text = compiled_missing_space_pattern.sub(r"\1. \2", str(text))
    for exception in exception_camelcases:
        exception_cleaned = compiled_missing_space_pattern.sub(r"\1. \2", exception)
        if exception_cleaned in text:
            text = text.replace(exception_cleaned, exception)

    return text


punctuation_replacement_rules = {
    # old patterns: replacement pattern
    # Convert bullet points to fullstops
    "[\u2022\u2023\u25E6\u2043\u2219*]": ".",
    r"[/:\\]": " ",  # Convert colon and forward and backward slashes to spaces
}

compiled_punct_patterns = {
    re.compile(p): v for p, v in punctuation_replacement_rules.items()
}


def replacements(text):
    """
    Ampersands and bullet points need some tweaking to be most useful in the pipeline.
    Some job adverts have different markers for a bullet pointed list. When this happens
    we want them to be in a fullstop separated format.
    e.g. ";• managing the grants database;• preparing financial and interna"
    ":•\xa0NMC registration paid every year•\xa0Free train"
    """
    text = (
        text.replace("&", "and")
        .replace("\xa0", " ")
        .replace("\n", ".")
        .replace("[", "")
        .replace("]", "")
    )

    for pattern, rep in compiled_punct_patterns.items():
        text = pattern.sub(rep, text)

    return text.strip()


def clean_text(text: str) -> List[str]:
    """Clean a job description by:
        - detecting camelcase
        - replacing punctuation
        - splitting into sentences

    Args:
        text (str): job description

    Returns:
        List[str]: List of cleaned job description sentences
    """
    return pipe(text, detect_camelcase, replacements)


def split_sentences(text: str) -> List[str]:
    """Splits job adverts into sentences.

    Splits on:
        - .?!

    Args:
        text str: job advert

    Returns:
        List[str]: A list of sentences
    """
    # split phrases on .?!
    pattern = re.compile(r"([.?!])\s+")
    # Split the text into sentences using the pattern
    sentences = re.split(pattern, text)

    return list(set(sentences))


def short_hash(text: str) -> int:
    """Create a short hash from a string

    Args:
        text (str): string to hash

    Returns:
        int: short hash
    """

    hx_code = md5(text.encode()).hexdigest()
    int_code = int(hx_code, 16)
    short_code = str(int_code)[:16]
    return int(short_code)
