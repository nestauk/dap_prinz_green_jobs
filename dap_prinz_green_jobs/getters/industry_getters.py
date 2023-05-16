import pandas as pd
import openpyxl


def load_companies_house() -> pd.DataFrame():
    """Downloads the Companies House dataset
    :return: A dataframe of company information including name and SIC
    :rtype: pd.DataFrame()
    """
    companies_house = pd.read_csv(
        "s3://prinz-green-jobs/inputs/data/industry_data/BasicCompanyDataAsOneFile-2023-05-01_key_columns_only.csv"
    )
    return companies_house


def load_industry_ghg() -> pd.DataFrame():
    """Downloads a dataset of greenhouse gas emissions per SIC
    :return: A dataframe of SIC and greenhouse gas emissions
    :rtype: pd.DataFrame()

    TO DO: clean this dataset - there are SIC codes in there like '20.12+20.2'
    """
    emissions_data = pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/industry_data/atmosphericemissionsghg.xlsx",
        sheet_name="GHG total",
        skiprows=3,
    )
    emissions_data.reset_index(inplace=True)
    emissions_data = emissions_data.loc[list(range(0, 21)) + list(range(26, 156))]

    emissions_data["Unnamed: 0"] = emissions_data["Unnamed: 0"].apply(
        lambda x: x if isinstance(x, str) else "0" + str(x) if x < 10 else str(x)
    )

    return emissions_data


def load_sic() -> pd.DataFrame():
    """Downloads a dataset of greenhouse gas emissions per SIC
    :return: A dataframe of SIC and greenhouse gas emissions
    :rtype: pd.DataFrame()
    """
    sic_data = pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/industry_data/publisheduksicsummaryofstructureworksheet.xlsx",
        sheet_name="reworked structure",
    )

    return sic_data
