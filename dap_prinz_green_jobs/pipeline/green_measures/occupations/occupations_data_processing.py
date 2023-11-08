import pandas as pd

import re


def process_job_title_soc(jobtitle_soc_data: pd.DataFrame()) -> pd.DataFrame():
    # Rename columns
    jobtitle_soc_data = jobtitle_soc_data.rename(
        columns={
            "SOC 2020 Ext Code": "SOC_2020_EXT",
            "SOC 2020": "SOC_2020",
            "SOC 2010": "SOC_2010",
        }
    )

    # Clean
    jobtitle_soc_data = jobtitle_soc_data[jobtitle_soc_data["SOC_2020"] != "}}}}"]

    return jobtitle_soc_data


def process_green_gla_soc(green_gla_data: pd.DataFrame()) -> pd.DataFrame():
    return green_gla_data.add_prefix("GLA_").rename(
        columns={"GLA_SOC2010 4-digit": "SOC_2010"}
    )


def process_green_timeshare_soc(green_timeshares: pd.DataFrame()) -> pd.DataFrame():
    return green_timeshares.add_prefix("timeshare_").rename(
        columns={"timeshare_SOC 2010 code": "SOC_2010"}
    )


major_places = [
    "Central London",
    "Midlands",
    "London",
    "Birmingham",
    "Leeds",
    "Glasgow",
    "Sheffield",
    "Bradford",
    "Manchester",
    "Edinburgh",
    "Liverpool",
    "Bristol",
    "Cardiff",
    "Coventry",
    "Nottingham",
    "Leicester",
    "Sunderland",
    "Belfast",
    "Newcastle",
    "Brighton",
    "Hull",
    "Plymouth",
    "Carlisle",
    "Berkshire",
    "Doncaster",
    "Bedford",
    "Chichester",
    "Wakefield",
]
lower_case_end_words = [
    "nights",
    "part time",
    "full time",
    "hybrid",
    "maternity cover",
    "remote",
    "self employed",
    "work from home",
    "benefits",
    "flexible",
    "Office Based",
]

lower_case_all_end_words = [
    word.lower() for word in major_places + lower_case_end_words
]


def job_title_cleaner(text, lower_case_all_end_words=lower_case_all_end_words):
    """
    Will apply a bunch of cleaning to a job title
    - removing certain things (locations or work type after a "-")
    - fixes some unicode &#163; -> £
    - Removes text after "£""

    Assumption: weird bad stuff comes after dashes or £ signs.
    So this won't work well for e.g "£30k Data Scientist" or "Remote - London Data Scientist"

    This isn't perfect, but should hopefully help quite a few examples

    Examples:
    'Part Home Based Block Manager - Chichester' -> 'Part Home Based Block Manager'
    'Employment Solicitor - Claimant - Leeds' -> 'Employment Solicitor - Claimant'
    'Retail Customer Service CSM 16hrs' -> 'Retail Customer Service CSM'
    'Bike Delivery Driver - London' -> 'Bike Delivery Driver'
    'Fulfillment Associate - &#163;1000 Sign on Bonus!' -> 'Fulfillment Associate'
    """
    if text:
        text = str(text)

        findreplace = {
            "&amp;": " and ",
            "&#160;": " ",
            "&#163;": "£",
            "(part time)": " ",
        }
        for f, r in findreplace.items():
            text = text.replace(f, r)
        # Get rid of any double + spaces
        text = re.sub(r"\s{2,}", " ", text).strip()

        # Remove mentions of hours e.g. Customer Service 30hrs -> Customer Service
        text = re.sub(r"\d+\s*hrs", "", text).strip()

        # If there is a "£" remove everything after it (e.g. £30k per annum)
        text = " ".join(text.split("£")[0:-1]).strip() if "£" in text else text

        # Remove certain things after the last dash
        if " - " in text:
            last_bit = text.split(" - ")[-1].strip().lower()
            # if any of the target words are in this, then remove everything after the dash
            # e.g "Data Scientist - remote, London" -> "Data Scientist"
            found = False
            for word in lower_case_all_end_words:
                if word in last_bit:
                    found = True
                    break
            if last_bit == "":  # This may happen if a £ was found
                found = True
            if found:
                # Remove everything after the lastedash
                text = " - ".join(text.split(" - ")[0:-1]).strip()

        if text[-1] == "-":
            text = text[0:-1].strip()

    return text
