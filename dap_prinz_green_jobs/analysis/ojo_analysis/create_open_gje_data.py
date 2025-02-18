"""
The creation of the dataset which will be downloadable on the Green Jobs Explorer website
"""

import pandas as pd

if __name__ == "__main__":
    output_location = "s3://nesta-open-data/green_jobs_explorer"

    data_agg_date = "20241121"

    occ_agg = pd.read_csv(
        f"s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/analysis/{data_agg_date}/occupation_aggregated_data_{data_agg_date}_extra_gjeformat.csv"
    )

    ind_agg = pd.read_csv(
        f"s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/analysis/{data_agg_date}/industry_aggregated_data_{data_agg_date}.csv"
    )

    itl_agg = pd.read_csv(
        f"s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/analysis/{data_agg_date}/all_itl_aggregated_data_{data_agg_date}.csv"
    )

    # Title sheet

    title_sheet = pd.DataFrame(
        {
            "Dataset title": ["Last updated", "Owner", "Contact"],
            "Green Jobs Explorer Data": [
                "21.11.2024",
                "Nesta",
                "dataanalytics@nesta.org.uk",
            ],
        }
    )

    # Occupation data

    occ_descr_sheet = pd.DataFrame(
        [
            {
                "Column name": "soc_code",
                "Extended description": "Standard Occupational Classification code.",
                "Addition information": "-",
            },
            {
                "Column name": "soc_name",
                "Extended description": "Standard Occupational Classification name.",
                "Addition information": "Corresponds to the Standard Occupational Classification code.",
            },
            {
                "Column name": "soc_description",
                "Extended description": "Standard Occupational Classification description.",
                "Addition information": "-",
            },
            {
                "Column name": "num_job_ads",
                "Extended description": "Number of job adverts for this occupation in our dataset.",
                "Addition information": "-",
            },
            {
                "Column name": "average_num_skills",
                "Extended description": "Average number of skills extracted from job adverts for this occupation.",
                "Addition information": "-",
            },
            {
                "Column name": "time_on_green_tasks",
                "Extended description": "The percentage of time spent on green tasks for this occupation.",
                "Addition information": "Calculated using estimates of the fraction of overall time spent doing green tasks (found on the ONS based on task-level data from the O*NET database in the US).",
            },
            {
                "Column name": "average_percentage_green_skills",
                "Extended description": "The average percentage green of skills extracted from job adverts for this occupation that were green.",
                "Addition information": 'This is the % of skills out of all the skills associated with the occupation that are "green", meaning that they were found in the ESCO Green Skill (2022) list.',
            },
            {
                "Column name": "average_industry_emissions",
                "Extended description": "Average per unit GHG emissions for industries job adverts for this occupation sit in.",
                "Addition information": "Average per unit GHG emissions for industries this occupation sits in (using ONS 2022 data on atmospheric emissions).",
            },
            {
                "Column name": "top_5_green_skills",
                "Extended description": "Most common green skills in job adverts for this occupation.",
                "Addition information": "For each job advert we extract the green skills it asks for. We display the 5 most commonly asked for green skills for this occupation.",
            },
            {
                "Column name": "top_5_not_green_skills",
                "Extended description": "Most common skills in job adverts for this occupation.",
                "Addition information": "For each job advert we extract the skills it asks for. We display the 5 most commonly asked for skills (green or not) for this occupation.",
            },
            {
                "Column name": "median_min_annualised_salary",
                "Extended description": "The median of the minimum annualised salary extracted from job adverts for this occupation.",
                "Addition information": "-",
            },
            {
                "Column name": "median_max_annualised_salary",
                "Extended description": "The median of the maximum annualised salary extracted from job adverts for this occupation.",
                "Addition information": "-",
            },
            {
                "Column name": "top_5_sics",
                "Extended description": "Top five most common Standard Industrial Codes for this occupation.",
                "Addition information": "-",
            },
            {
                "Column name": "top_5_itl2_quotient",
                "Extended description": "Top 5 ITL2 quotients for this occupation.",
                "Addition information": "The top 5 regions which have above average proportions of job adverts for this occupation. The quotient is given alongside the region ITL 2 name - this is the proportion of job adverts from this occupation in this region, divided by the proportion of all job adverts in this region.",
            },
            {
                "Column name": "green_occupation_rating",
                "Extended description": "Green occupation rating.",
                "Addition information": "This rating is based on time spent on green tasks for this occupation. Higher is better. We remove outliers, then divide the values for each measure into 3 equally spaced intervals.",
            },
            {
                "Column name": "green_industry_rating",
                "Extended description": "Green industry rating.",
                "Addition information": "This is based on industry emissions for this occupation. Higher is better. We remove outliers, then divide the values for each measure into 3 equally spaced intervals.",
            },
            {
                "Column name": "green_skills_rating",
                "Extended description": "Green skills rating.",
                "Addition information": "This is based on the % of green skills for this occupation. Higher is better. We remove outliers, then divide the values for each measure into 3 equally spaced intervals.",
            },
            {
                "Column name": "top_5_similar_occs",
                "Extended description": "The five most similar occupations based on skills asked for.",
                "Addition information": "-",
            },
            {
                "Column name": "yearly_data",
                "Extended description": "Summary information about greenness measures per year for this occupation.",
                "Addition information": "-",
            },
        ]
    )

    occ_agg["average_percentage_green_skills"] = (
        occ_agg["average_prop_green_skills"] * 100
    )

    # There was a weird issue when I saved datetime objects in the yearly_data column, it turned 2021 to 51, 2022 to 52, 2023 to 53 and 2024 to 54!
    occ_agg["yearly_data"] = occ_agg["yearly_data"].apply(
        lambda x: x.replace("'year': 50", "'year': 2020")
        .replace("'year': 51", "'year': 2021")
        .replace("'year': 52", "'year': 2022")
        .replace("'year': 53", "'year': 2023")
        .replace("'year': 54", "'year': 2024")
    )

    occ_agg_cleaned = occ_agg.rename(
        columns={
            "SOC_2020_EXT": "soc_code",
            "average_occ_green_timeshare": "time_on_green_tasks",
            "average_ind_perunit_ghg": "average_industry_emissions",
            "clean_soc_name": "soc_name",
            "occ_greenness": "green_occupation_rating",
            "ind_greenness": "green_industry_rating",
            "skills_greenness": "green_skills_rating",
        }
    )

    occ_agg_cleaned = occ_agg_cleaned[
        [
            "soc_code",
            "soc_name",
            "soc_description",
            "num_job_ads",
            "average_num_skills",
            "time_on_green_tasks",
            "average_percentage_green_skills",
            "average_industry_emissions",
            "top_5_green_skills",
            "top_5_not_green_skills",
            "median_min_annualised_salary",
            "median_max_annualised_salary",
            "top_5_sics",
            "top_5_itl2_quotient",
            "green_occupation_rating",
            "green_industry_rating",
            "green_skills_rating",
            "top_5_similar_occs",
            "yearly_data",
        ]
    ]

    with pd.ExcelWriter(
        f"{output_location}/occupation_aggregated_data_{data_agg_date}_GJE.xlsx"
    ) as writer:
        title_sheet.to_excel(writer, sheet_name="README", index=False)
        occ_descr_sheet.to_excel(writer, sheet_name="Descriptions", index=False)
        occ_agg_cleaned.to_excel(writer, sheet_name="Data", index=False)

    # Industry

    ind_descr_sheet = pd.DataFrame(
        [
            {
                "Column name": "sic_code",
                "Extended description": "Standard Industrial Classification code.",
                "Addition information": "-",
            },
            {
                "Column name": "sic_name",
                "Extended description": "Standard Industrial Classification name.",
                "Addition information": "Corresponds to the Standard Industrial Classification code.",
            },
            {
                "Column name": "num_job_ads",
                "Extended description": "Number of job adverts for this industry in our dataset.",
                "Addition information": "-",
            },
            {
                "Column name": "average_num_skills",
                "Extended description": "Average number of skills extracted from job adverts for this industry.",
                "Addition information": "-",
            },
            {
                "Column name": "average_time_on_green_tasks",
                "Extended description": "The average percentage of time spent on green tasks for this industry.",
                "Addition information": "Calculated using estimates of the fraction of overall time spent doing green tasks (found on the ONS based on task-level data from the O*NET database in the US).",
            },
            {
                "Column name": "average_percentage_green_skills",
                "Extended description": "The average percentage green of skills extracted from job adverts for this industry that were green.",
                "Addition information": 'This is the % of skills out of all the skills associated with the industry that are "green", meaning that they were found in the ESCO Green Skill (2022) list.',
            },
            {
                "Column name": "average_industry_emissions",
                "Extended description": "Average per unit GHG emissions for this industry.",
                "Addition information": "Average per unit GHG emissions for this industry (using ONS 2022 data on atmospheric emissions).",
            },
            {
                "Column name": "top_5_green_skills",
                "Extended description": "Most common green skills in job adverts for this industry.",
                "Addition information": "For each job advert we extract the green skills it asks for. We display the 5 most commonly asked for green skills for this industry.",
            },
            {
                "Column name": "top_5_not_green_skills",
                "Extended description": "Most common skills in job adverts for this industry.",
                "Addition information": "For each job advert we extract the skills it asks for. We display the 5 most commonly asked for skills (green or not) for this industry.",
            },
            {
                "Column name": "median_min_annualised_salary",
                "Extended description": "The median of the minimum annualised salary extracted from job adverts for this industry.",
                "Addition information": "-",
            },
            {
                "Column name": "median_max_annualised_salary",
                "Extended description": "The median of the maximum annualised salary extracted from job adverts for this industry.",
                "Addition information": "-",
            },
            {
                "Column name": "top_5_socs",
                "Extended description": "Top five most common Standard Occupational Codes for this industry.",
                "Addition information": "-",
            },
            {
                "Column name": "top_5_itl2_quotient",
                "Extended description": "Top 5 ITL2 quotients for this industry.",
                "Addition information": "The top 5 regions which have above average proportions of job adverts for this industry. The quotient is given alongside the region ITL 2 name - this is the proportion of job adverts from this industry in this region, divided by the proportion of all job adverts in this region.",
            },
            {
                "Column name": "green_occupation_rating",
                "Extended description": "Green occupation rating.",
                "Addition information": "This rating is based on time spent on green tasks for this industry. Higher is better. We remove outliers, then divide the values for each measure into 3 equally spaced intervals.",
            },
            {
                "Column name": "green_industry_rating",
                "Extended description": "Green industry rating.",
                "Addition information": "This is based on industry emissions for this industry. Higher is better. We remove outliers, then divide the values for each measure into 3 equally spaced intervals.",
            },
            {
                "Column name": "green_skills_rating",
                "Extended description": "Green skills rating.",
                "Addition information": "This is based on the % of green skills for this industry. Higher is better. We remove outliers, then divide the values for each measure into 3 equally spaced intervals.",
            },
            {
                "Column name": "yearly_data",
                "Extended description": "Summary information about greenness measures per year for this industry.",
                "Addition information": "-",
            },
        ]
    )

    ind_agg["average_percentage_green_skills"] = (
        ind_agg["average_prop_green_skills"] * 100
    )

    # There was a weird issue when I saved datetime objects in the yearly_data column, it turned 2021 to 51, 2022 to 52, 2023 to 53 and 2024 to 54!
    ind_agg["yearly_data"] = ind_agg["yearly_data"].apply(
        lambda x: x.replace("'year': 50", "'year': 2020")
        .replace("'year': 51", "'year': 2021")
        .replace("'year': 52", "'year': 2022")
        .replace("'year': 53", "'year': 2023")
        .replace("'year': 54", "'year': 2024")
    )

    ind_agg_cleaned = ind_agg.rename(
        columns={
            "SIC": "sic_code",
            "SIC_name": "sic_name",
            "average_occ_green_timeshare": "average_time_on_green_tasks",
            "average_ind_perunit_ghg": "average_industry_emissions",
            "occ_greenness": "green_occupation_rating",
            "ind_greenness": "green_industry_rating",
            "skills_greenness": "green_skills_rating",
        }
    )

    ind_agg_cleaned = ind_agg_cleaned[
        [
            "sic_code",
            "sic_name",
            "num_job_ads",
            "average_num_skills",
            "average_time_on_green_tasks",
            "average_percentage_green_skills",
            "average_industry_emissions",
            "top_5_green_skills",
            "top_5_not_green_skills",
            "median_min_annualised_salary",
            "median_max_annualised_salary",
            "top_5_socs",
            "top_5_itl2_quotient",
            "green_occupation_rating",
            "green_industry_rating",
            "green_skills_rating",
            "yearly_data",
        ]
    ]

    with pd.ExcelWriter(
        f"{output_location}/industry_aggregated_data_{data_agg_date}_GJE.xlsx"
    ) as writer:
        title_sheet.to_excel(writer, sheet_name="README", index=False)
        ind_descr_sheet.to_excel(writer, sheet_name="Descriptions", index=False)
        ind_agg_cleaned.to_excel(writer, sheet_name="Data", index=False)

    # Region

    itl_descr_sheet = pd.DataFrame(
        [
            {
                "Column name": "itl_code",
                "Extended description": "International Territorial Level code.",
                "Addition information": "-",
            },
            {
                "Column name": "itl_name",
                "Extended description": "International Territorial Level name.",
                "Addition information": "Corresponds to the International Territorial Level code.",
            },
            {
                "Column name": "itl_type",
                "Extended description": "International Territorial Level hierarchy level (1 - least granular, 2 - mid, or 3 - most granular).",
                "Addition information": "Detail on ITL types can be found here: https://www.ons.gov.uk/methodology/geography/ukgeographies/eurostat",
            },
            {
                "Column name": "num_job_ads",
                "Extended description": "Number of job adverts for this region in our dataset.",
                "Addition information": "-",
            },
            {
                "Column name": "average_num_skills",
                "Extended description": "Average number of skills extracted from job adverts for this region.",
                "Addition information": "-",
            },
            {
                "Column name": "average_time_on_green_tasks",
                "Extended description": "The average percentage of time spent on green tasks for this region.",
                "Addition information": "Calculated using estimates of the fraction of overall time spent doing green tasks (found on the ONS based on task-level data from the O*NET database in the US).",
            },
            {
                "Column name": "average_percentage_green_skills",
                "Extended description": "The average percentage green of skills extracted from job adverts for this region that were green.",
                "Addition information": 'This is the % of skills out of all the skills associated with the region that are "green", meaning that they were found in the ESCO Green Skill (2022) list.',
            },
            {
                "Column name": "average_industry_emissions",
                "Extended description": "Average per unit GHG emissions for this region.",
                "Addition information": "Average per unit GHG emissions for this region (using ONS 2022 data on atmospheric emissions).",
            },
            {
                "Column name": "top_5_green_skills",
                "Extended description": "Most common green skills in job adverts for this region.",
                "Addition information": "For each job advert we extract the green skills it asks for. We display the 5 most commonly asked for green skills for this region.",
            },
            {
                "Column name": "top_5_not_green_skills",
                "Extended description": "Most common skills in job adverts for this region.",
                "Addition information": "For each job advert we extract the skills it asks for. We display the 5 most commonly asked for skills (green or not) for this region.",
            },
            {
                "Column name": "median_min_annualised_salary",
                "Extended description": "The median of the minimum annualised salary extracted from job adverts for this region",
                "Addition information": "-",
            },
            {
                "Column name": "median_max_annualised_salary",
                "Extended description": "The median of the maximum annualised salary extracted from job adverts for this region.",
                "Addition information": "-",
            },
            {
                "Column name": "top_5_socs",
                "Extended description": "Top five most common Standard Occupational Codes for this region.",
                "Addition information": "-",
            },
            {
                "Column name": "top_5_sics",
                "Extended description": "Top five most common Standard Industrial Codes for this region.",
                "Addition information": "-",
            },
            {
                "Column name": "green_occupation_rating",
                "Extended description": "Green occupation rating.",
                "Addition information": "This rating is based on time spent on green tasks for this region. We remove outliers, then divide the values for each measure into 3 equally spaced intervals.",
            },
            {
                "Column name": "green_industry_rating",
                "Extended description": "Green industry rating.",
                "Addition information": "This is based on industry emissions for this region. Higher is better. Higher is better. We remove outliers, then divide the values for each measure into 3 equally spaced intervals.",
            },
            {
                "Column name": "green_skills_rating",
                "Extended description": "Green skills rating.",
                "Addition information": "This is based on the % of green skills for this region. Higher is better. We remove outliers, then divide the values for each measure into 3 equally spaced intervals.",
            },
            {
                "Column name": "yearly_data",
                "Extended description": "Summary information about greenness measures per year for this region",
                "Addition information": "-",
            },
        ]
    )

    itl_agg["average_percentage_green_skills"] = (
        itl_agg["average_prop_green_skills"] * 100
    )

    # There was a weird issue when I saved datetime objects in the yearly_data column, it turned 2021 to 51, 2022 to 52, 2023 to 53 and 2024 to 54!
    itl_agg["yearly_data"] = itl_agg["yearly_data"].apply(
        lambda x: x.replace("'year': 50", "'year': 2020")
        .replace("'year': 51", "'year': 2021")
        .replace("'year': 52", "'year': 2022")
        .replace("'year': 53", "'year': 2023")
        .replace("'year': 54", "'year': 2024")
    )

    itl_agg_cleaned = itl_agg.rename(
        columns={
            "itl_level": "itl_type",
            "average_occ_green_timeshare": "average_time_on_green_tasks",
            "average_ind_perunit_ghg": "average_industry_emissions",
            "occ_greenness": "green_occupation_rating",
            "ind_greenness": "green_industry_rating",
            "skills_greenness": "green_skills_rating",
        }
    )

    itl_agg_cleaned = itl_agg_cleaned[
        [
            "itl_code",
            "itl_name",
            "itl_type",
            "num_job_ads",
            "average_num_skills",
            "average_time_on_green_tasks",
            "average_percentage_green_skills",
            "average_industry_emissions",
            "top_5_green_skills",
            "top_5_not_green_skills",
            "median_min_annualised_salary",
            "median_max_annualised_salary",
            "top_5_socs",
            "top_5_sics",
            "green_occupation_rating",
            "green_industry_rating",
            "green_skills_rating",
            "yearly_data",
        ]
    ]

    with pd.ExcelWriter(
        f"{output_location}/region_aggregated_data_{data_agg_date}_GJE.xlsx"
    ) as writer:
        title_sheet.to_excel(writer, sheet_name="README", index=False)
        itl_descr_sheet.to_excel(writer, sheet_name="Descriptions", index=False)
        itl_agg_cleaned.to_excel(writer, sheet_name="Data", index=False)
