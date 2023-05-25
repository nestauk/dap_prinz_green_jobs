import pandas as pd


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
