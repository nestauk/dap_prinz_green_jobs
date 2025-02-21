"""
Script to read in job advert data and create datasets used in Flourish for analysis of Welsh job adverts.
"""

import polars as pl
import geopandas as gpd
import pandas as pd

from dap_prinz_green_jobs import PROJECT_DIR
from dap_prinz_green_jobs.utils.chloropleth_utils import get_nuts3polygons_dict

import os

job_advert_s3_location = "s3://prinz-green-jobs/outputs/data/ojo_application"

created_date_file = os.path.join(
    job_advert_s3_location,
    "deduplicated_sample/20241114/latest_update_20241114_key_columns.parquet",
)

combined_green_measures_filename = os.path.join(
    job_advert_s3_location,
    "extracted_green_measures/analysis/20241121/combined_green_measures_and_meta.parquet",
)

output_directory = os.path.join(PROJECT_DIR, "outputs/analysis/welsh_analysis/")
if not os.path.exists(output_directory):
    os.makedirs(output_directory)


def get_itl3_polygons() -> dict:
    """
    Retrieve the polygon data for ITL3 regions
    """

    nuts3polygons_dict = get_nuts3polygons_dict()
    itl3polygons_dict = {
        k.replace("UK", "TL"): v for k, v in nuts3polygons_dict.items()
    }

    return itl3polygons_dict


def process_date_columns(data: pl.DataFrame) -> pl.DataFrame:
    """
    Take the 'created' column and process it into separate columns
    for month, year, and quarter
    """

    data = data.with_columns(
        pl.col("created").dt.date().alias("date"),
    )

    data = data.with_columns(
        (pl.col("date").dt.month_start()).alias("month"),
        (pl.col("date").dt.year()).alias("year"),
        (pl.col("date").dt.quarter()).alias("quarter"),
    )

    data = data.with_columns(
        (
            pl.col("year").cast(pl.String) + " Q" + pl.col("quarter").cast(pl.String)
        ).alias("y_quarter")
    )
    return data


def get_missing_proportions(data: pl.DataFrame):
    a = data["SOC_2020_name"].is_null().sum() * 100 / len(data)
    print(f"{round(a,3)}% of job adverts have SOCs missing")
    b = data["SIC_2_digit"].is_null().sum() * 100 / len(data)
    print(f"{round(b,3)}% of job adverts have SICs missing")


def get_green_averages(df: pl.DataFrame, group_by_col: str) -> pl.DataFrame:
    """
    For an inputted grouping of the data (specified by group_by_col, e.g. year)
    calculate the average of each green measure, as well as the number of job adverts.

    Format this in such a way (unpivot) that the average values are per row, not per column.
    """
    grouped_df = df.group_by(group_by_col, maintain_order=True).agg(
        pl.col("PROP_GREEN").mean().alias("average_prop_green_skills"),
        pl.col("INDUSTRY GHG PER UNIT EMISSIONS").mean().alias("average_ghg"),
        pl.col("GREEN TIMESHARE").mean().alias("average_green_timeshare"),
        pl.col("job_id").n_unique().alias("number_job_ids"),
    )

    return grouped_df.unpivot(
        on=[
            "average_prop_green_skills",
            "average_ghg",
            "average_green_timeshare",
            "number_job_ids",
        ],
        index=group_by_col,
    )


def join_time_averages(
    combined_all_data: pl.DataFrame,
    welsh_combined_all_data: pl.DataFrame,
    england_combined_all_data: pl.DataFrame,
    scotland_combined_all_data: pl.DataFrame,
    group_by_col: str = "year",
) -> pl.DataFrame:
    """
    Join up the averaged data for all nations and all data into one dataset.
    """

    all_averages_per_group = get_green_averages(combined_all_data, group_by_col).rename(
        {"value": "All"}
    )
    welsh_averages_per_group = get_green_averages(
        welsh_combined_all_data, group_by_col
    ).rename({"value": "Wales"})
    england_averages_per_group = get_green_averages(
        england_combined_all_data, group_by_col
    ).rename({"value": "England"})
    scotland_averages_per_group = get_green_averages(
        scotland_combined_all_data, group_by_col
    ).rename({"value": "Scotland"})

    joined_grouped_averages = all_averages_per_group.join(
        welsh_averages_per_group, on=[group_by_col, "variable"]
    )
    joined_grouped_averages = joined_grouped_averages.join(
        england_averages_per_group, on=[group_by_col, "variable"]
    )
    joined_grouped_averages = joined_grouped_averages.join(
        scotland_averages_per_group, on=[group_by_col, "variable"]
    )

    return joined_grouped_averages


def get_grouped_perc_green_jobs(data: pl.DataFrame, group_by_col="year"):
    """
    Get the percentages of the ONET green/not green categories
    for a dataset grouped by group_by_col
    """

    grouped_df = data.group_by(group_by_col, maintain_order=True).agg(
        (pl.col("GREEN/NOT GREEN") == "Non-green").sum().alias("Number non-green"),
        (pl.col("GREEN/NOT GREEN") == "Green").sum().alias("Number green"),
        (pl.col("GREEN/NOT GREEN").is_null()).sum().alias("Number with no SOC"),
        pl.col("GREEN/NOT GREEN").len().alias("Total number"),
    )

    grouped_df = grouped_df.with_columns(
        (pl.col("Number non-green") * 100 / pl.col("Total number")).alias(
            "% non-green"
        ),
        (pl.col("Number green") * 100 / pl.col("Total number")).alias("% green"),
        (pl.col("Number with no SOC") * 100 / pl.col("Total number")).alias("% no soc"),
    )

    return grouped_df


if __name__ == "__main__":
    #### Inputs and data processing ####

    # Read in data
    created_date = pl.read_parquet(created_date_file)
    combined_all_data_orig = pl.read_parquet(combined_green_measures_filename)

    # Add creation date
    combined_all_data = combined_all_data_orig.join(
        created_date["id", "created"],
        left_on="job_id",
        right_on="id",
        how="left",
    ).drop("__index_level_0__")

    combined_all_data = process_date_columns(combined_all_data)

    # Define country based off ITL1 name, and separate out data for each country
    combined_all_data = combined_all_data.with_columns(
        pl.when(
            pl.col("itl_1_name").is_in(
                [
                    "North East (England)",
                    "London",
                    "South West (England)",
                    "East Midlands (England)",
                    "West Midlands (England)",
                    "East of England",
                    "South East (England)",
                    "Northern Ireland",
                    "North West (England)",
                    "Yorkshire and the Humber",
                ]
            )
        )
        .then(pl.lit("England"))
        .otherwise(pl.col("itl_1_name"))
        .alias("country")
    )

    welsh_combined_all_data = combined_all_data.filter(pl.col("itl_1_name") == "Wales")

    england_combined_all_data = combined_all_data.filter(pl.col("country") == "England")

    scotland_combined_all_data = combined_all_data.filter(
        pl.col("itl_1_name") == "Scotland"
    )

    #### High level analysis ####

    print(
        f"{len(welsh_combined_all_data)*100/len(combined_all_data)}% of job adverts ({len(welsh_combined_all_data)}) are from Wales"
    )
    print(
        f"{len(england_combined_all_data)*100/len(combined_all_data)}% of job adverts ({len(england_combined_all_data)}) are from England"
    )
    print(
        f"{len(scotland_combined_all_data)*100/len(combined_all_data)}% of job adverts ({len(scotland_combined_all_data)}) are from Scotland"
    )

    num_null = len(combined_all_data.filter(pl.col("itl_1_name").is_null()))
    print(
        f"{num_null*100/len(combined_all_data)}% of job adverts don't have a ITL 1 given, in some cases these will be from Wales"
    )

    print("In Wales:")
    get_missing_proportions(welsh_combined_all_data)

    print("In England:")
    get_missing_proportions(england_combined_all_data)

    #### RQ: Has there been an increase in green job offers? ####
    # Find trends over time grouped by year, month and quarter

    joined_yearly_averages = join_time_averages(
        combined_all_data,
        welsh_combined_all_data,
        england_combined_all_data,
        scotland_combined_all_data,
        group_by_col="year",
    )
    joined_yearly_averages.write_csv(
        os.path.join(output_directory, "joined_yearly_averages.csv")
    )

    # Just 2024 data in format for grouped column chart
    joined_yearly_averages.filter(pl.col("year") == 2024).rename(
        {"variable": "variable_x"}
    ).unpivot(
        on=["All", "Wales", "England", "Scotland"], index=["year", "variable_x"]
    ).rename(
        {"variable_x": "variable", "variable": "Country"}
    ).write_csv(
        os.path.join(output_directory, "2024_averages.csv")
    )

    joined_monthly_averages = join_time_averages(
        combined_all_data,
        welsh_combined_all_data,
        england_combined_all_data,
        scotland_combined_all_data,
        group_by_col="month",
    )
    # Don't include lastest month in output since it was an incomplete month for data collection
    joined_monthly_averages.filter(pl.col("month") < pl.date(2024, 11, 1)).write_csv(
        os.path.join(output_directory, "joined_monthly_averages.csv")
    )

    joined_quarterly_averages = join_time_averages(
        combined_all_data,
        welsh_combined_all_data,
        england_combined_all_data,
        scotland_combined_all_data,
        group_by_col="y_quarter",
    )
    # Don't include first or last quarter output since they were incomplete months for data collection
    # Don't include Q4 2022 or Q1 2023 since there was a problem collecting data in these months so the numbers are low
    joined_quarterly_averages.filter(
        ~pl.col("y_quarter").is_in(["2020 Q4", "2024 Q4", "2022 Q4", "2023 Q1"])
    ).write_csv(os.path.join(output_directory, "joined_quarter_averages.csv"))

    #### RQ: How many green jobs are available in Wales and what share of
    # the job market does this represent? ####
    # Proportion of green jobs

    grouped_by_q_wales = get_grouped_perc_green_jobs(
        welsh_combined_all_data, group_by_col="y_quarter"
    )
    grouped_by_q_wales.write_csv(
        os.path.join(output_directory, "grouped_green_not_green_props.csv")
    )

    grouped_by_year_wales = get_grouped_perc_green_jobs(
        welsh_combined_all_data, group_by_col="year"
    )
    grouped_by_year_wales.filter(pl.col("year") != 2020).write_csv(
        os.path.join(output_directory, "grouped_green_not_green_props_year.csv")
    )

    grouped_df_by_country = get_grouped_perc_green_jobs(
        combined_all_data, group_by_col="country"
    )
    grouped_df_by_country.filter(~pl.col("country").is_null()).write_csv(
        os.path.join(output_directory, "grouped_green_not_green_props_country.csv")
    )

    grouped_by_itl3_wales = get_grouped_perc_green_jobs(
        welsh_combined_all_data, group_by_col="itl_3_name"
    )
    grouped_by_itl3_wales.filter(~pl.col("itl_3_name").is_null()).write_csv(
        os.path.join(output_directory, "grouped_green_not_green_props_itl3.csv")
    )

    #### RQ: Within Wales, what is the geography of green job offers? ####

    # region_grouped_df_pl = (welsh_combined_all_data.group_by("itl_3_code", maintain_order=True)
    #      .agg(
    #          pl.col("PROP_GREEN").mean().alias("average_prop_green_skills"),
    #          pl.col('INDUSTRY GHG PER UNIT EMISSIONS').mean().alias("average_ghg"),
    #          pl.col('GREEN TIMESHARE').mean().alias("average_green_timeshare"),
    #          pl.col('job_id').n_unique().alias("number_job_ids"),
    #      ))

    # region_grouped_df = region_grouped_df_pl.to_pandas()

    # itl3polygons_dict = get_itl3_polygons()
    # region_grouped_df['geometry_name'] = region_grouped_df["itl_3_code"].map(itl3polygons_dict)
    # region_grouped_df[['geometry', 'itl_name']] = region_grouped_df['geometry_name'].apply(lambda x: pd.Series(x))
    # region_grouped_df.drop('geometry_name', axis=1, inplace=True)

    # geo_df = gpd.GeoDataFrame(region_grouped_df)
    # geo_df.to_file(os.path.join(output_directory, "choropleth_data.geojson"), driver="GeoJSON")

    itl_3_name_averages = get_green_averages(welsh_combined_all_data, "itl_3_name")
    itl_3_name_averages.write_csv(
        os.path.join(output_directory, "itl_3_name_averages.csv")
    )

    #### RQ: What is the relationship between green measures, occupations and salaries? ####

    group_by_col = "SOC_2020_name"
    welsh_grouped_by_occ = welsh_combined_all_data.group_by(
        group_by_col, maintain_order=True
    ).agg(
        pl.col("PROP_GREEN").mean().alias("average_prop_green_skills"),
        pl.col("INDUSTRY GHG PER UNIT EMISSIONS").mean().alias("average_ghg"),
        pl.col("GREEN TIMESHARE").mean().alias("average_green_timeshare"),
        pl.col("job_id").n_unique().alias("number_job_ids"),
        pl.col("max_annualised_salary").mean().alias("average_max_annualised_salary"),
    )

    soc_2_onet = dict(
        zip(
            welsh_combined_all_data["SOC_2020_name"],
            welsh_combined_all_data["GREEN/NOT GREEN"],
        )
    )

    welsh_grouped_by_occ = welsh_grouped_by_occ.with_columns(
        pl.col("SOC_2020_name")
        .replace_strict(soc_2_onet, default=None)
        .alias("GREEN/NOT GREEN")
    )

    # Only include stats for occupations with at least 50 job adverts, otherwise averages are inaccurate
    welsh_grouped_by_occ.filter(
        (~pl.col("SOC_2020_name").is_null()) & (pl.col("number_job_ids") > 50)
    ).write_csv(os.path.join(output_directory, "welsh_grouped_by_occ.csv"))
