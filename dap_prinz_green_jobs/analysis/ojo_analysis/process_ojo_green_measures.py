"""
Load the green measures from a sample of OJO data and process it into a form needed to create analysis from
"""

from dap_prinz_green_jobs import BUCKET_NAME, logger
from dap_prinz_green_jobs.getters.data_getters import load_s3_data
from dap_prinz_green_jobs.getters.industry_getters import load_sic
from dap_prinz_green_jobs.getters.ojo_getters import (
    get_mixed_ojo_location_sample,
    get_mixed_ojo_salaries_sample,
)
from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupations_measures_utils import (
    OccupationMeasures,
)

import pandas as pd
import numpy as np
import altair as alt
from tqdm import tqdm

import os


# clean up skills
def merge_ents(ents):
    if not isinstance(ents, list):
        return None

    elif "green" in ents[1]:
        return [ents[0]] + [ents[1][0]] + [ents[1][1]] + ents[1][2]
    else:
        return ents[0] + [ents[1]]


# function to clean SOC names
def clean_soc_name(soc_name):
    if soc_name:
        return soc_name.replace("n.e.c.", "").strip()
    else:
        return None


def load_ojo_green_measures(
    production, config, skills_date_stamp, occ_date_stamp, ind_date_stamp
):
    green_skills_outputs = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/ojo_application/extracted_green_measures/{skills_date_stamp}/ojo_sample_skills_green_measures_production_{production}_{config}.json",
    )

    green_occs_outputs = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/ojo_application/extracted_green_measures/{occ_date_stamp}/ojo_sample_occupation_green_measures_production_{production}_{config}.json",
    )
    soc_name_dict = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/ojo_application/extracted_green_measures/20231110/soc_name_dict.json",  # TO CHANGE TO THE SAME DATESTAMP as occ_date_stamp
    )
    green_inds_outputs = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/ojo_application/extracted_green_measures/{ind_date_stamp}/ojo_sample_industry_green_measures_production_{production}_{config}.json",
    )

    skill_measures_df = (
        pd.DataFrame.from_dict(green_skills_outputs, orient="index")
        .reset_index()
        .rename(columns={"index": "job_id"})
    )
    occs_measures_df = (
        pd.DataFrame.from_dict(green_occs_outputs, orient="index")
        .reset_index()
        .rename(columns={"index": "job_id"})
    )
    inds_measures_df = (
        pd.DataFrame.from_dict(green_inds_outputs, orient="index")
        .reset_index()
        .rename(columns={"index": "job_id"})
    )

    return skill_measures_df, occs_measures_df, inds_measures_df, soc_name_dict


def merge_green_measures(
    skill_measures_df,
    occs_measures_df,
    inds_measures_df,
    soc_name_dict,
    job_id_col="job_id",
):
    soc_2020_6_dict = soc_name_dict["soc_2020_6"]
    soc_2020_4_dict = soc_name_dict["soc_2020_4"]

    all_green_measures_df = pd.merge(
        skill_measures_df, occs_measures_df, how="outer", on=job_id_col
    )
    all_green_measures_df = pd.merge(
        all_green_measures_df, inds_measures_df, how="outer", on=job_id_col
    )
    # replace float with 0
    all_green_measures_df = all_green_measures_df.fillna("")
    all_green_measures_df["NUM_GREEN_ENTS"] = all_green_measures_df["GREEN_ENTS"].apply(
        len
    )
    # Separate out the SOC columns
    for soc_columns in ["SOC_2020_EXT", "SOC_2020", "SOC_2010", "name"]:
        all_green_measures_df[soc_columns] = all_green_measures_df["SOC"].apply(
            lambda x: x[soc_columns] if x else None
        )
    all_green_measures_df.drop(columns=["SOC"], inplace=True)

    all_green_measures_df.rename(
        columns={"name": "SOC_names", job_id_col: "job_id"}, inplace=True
    )
    all_green_measures_df["SOC_2020_name"] = all_green_measures_df["SOC_2020"].map(
        soc_2020_4_dict
    )
    all_green_measures_df["SOC_2020_EXT_name"] = all_green_measures_df[
        "SOC_2020_EXT"
    ].map(soc_2020_6_dict)

    all_green_measures_df.rename(columns={})
    all_green_measures_df.replace("", np.nan, inplace=True)

    # weird thing in industry measures. 3 times
    all_green_measures_df = all_green_measures_df[
        all_green_measures_df["INDUSTRY GHG PER UNIT EMISSIONS"] != ":"
    ]

    # For multiskills the format is different in ENTS - separate them out
    # ENTS: [[['research'], 'SKILL'], [['This is a cut up', 'a cut up sentence'], 'MULTISKILL']]
    # to [[['research'], 'SKILL'], [['This is a cut up'], 'SKILL'], [['a cut up sentence'], 'SKILL']]

    def separate_multiskill_ents(entlist):
        separate_ents = []
        for ent in entlist:
            if ent[1] == "MULTISKILL":
                for sep_ent in ent[0]:
                    separate_ents.append(
                        [[sep_ent], "MULTISKILL"]
                    )  # The format it is expecting
            else:
                separate_ents.append(ent)
        return separate_ents

    all_green_measures_df["ENTS"] = all_green_measures_df["ENTS"].apply(
        lambda x: separate_multiskill_ents(x) if isinstance(x, list) else x
    )

    print(f"There are {len(all_green_measures_df)} rows in the merged data")
    print(f"There are {all_green_measures_df['job_id'].nunique()} unique job ids")

    return all_green_measures_df


def add_salaries(all_green_measures_df, job_id_col="job_id"):
    salary_information = get_mixed_ojo_salaries_sample()
    salary_information[job_id_col] = salary_information.id.astype(str)

    # add salary
    all_green_measures_df = pd.merge(
        all_green_measures_df, salary_information, on=job_id_col, how="left"
    )
    return all_green_measures_df


def add_locations(all_green_measures_df, job_id_col="job_id"):
    locations_information = get_mixed_ojo_location_sample()
    locations_information[job_id_col] = locations_information.id.astype(str)

    # add locations
    locations_information = locations_information.drop(
        columns=["is_uk", "is_large_geo", "location", "coordinates"]
    )
    all_green_measures_df = pd.merge(
        all_green_measures_df, locations_information, on=job_id_col, how="left"
    )
    return all_green_measures_df


def add_sic_info(all_green_measures_df):
    sic_data = load_sic()
    sic_names = dict(
        zip(sic_data["Division"].tolist(), sic_data["Description"].tolist())
    )

    # Add these new columns
    all_green_measures_df["SIC_2_digit"] = all_green_measures_df["SIC"].apply(
        lambda x: str(x)[0:2] if x else None
    )
    all_green_measures_df["SIC_2_digit_name"] = all_green_measures_df[
        "SIC_2_digit"
    ].apply(lambda x: sic_names.get(x) if x else None)

    return all_green_measures_df


def get_soc_info():
    # we need soc descriptions as well - they have descriptions for 6 digit sic codes
    soc_descriptions = pd.read_excel(
        "s3://prinz-green-jobs/inputs/data/occupation_data/ons/extendedsoc2020structureanddescriptionsexcel121023.xlsx",
        sheet_name="Extended SOC descriptions MG1-9",
    )
    soc_descriptions.columns = [
        i.lower().strip().replace(" ", "_").replace("-", "")
        for i in soc_descriptions.iloc[0].values
    ]
    soc_descriptions.drop(0, inplace=True)
    soc_descriptions = (
        soc_descriptions[["subunit_group", "group_title", "descriptions"]]
        # drop na if its na in any of the columns
        .dropna(subset=["subunit_group", "group_title", "descriptions"])
    )
    # clean description
    soc_descriptions["clean_description"] = soc_descriptions.descriptions.apply(
        lambda x: x.replace("\n", " ")
        .replace("\r", " ")
        .replace("\t", " ")
        .replace("  ", " ")
        .strip()
        .replace("\xa0", "")
    )
    soc_descriptions_dict = soc_descriptions.set_index(
        "subunit_group"
    ).clean_description.to_dict()

    return soc_descriptions_dict


def add_green_topics(all_green_measures_df):
    om = OccupationMeasures()
    om.load()

    all_green_measures_df["green_topics_lists"] = all_green_measures_df[
        "SOC_2010"
    ].apply(
        lambda x: om.soc_green_measures_dict.get(x)["ONET_green_topics"]
        if x in om.soc_green_measures_dict
        else None
    )

    return all_green_measures_df


def add_additional_metadata(all_green_measures_df, job_id_col="job_id"):
    all_green_measures_df = add_salaries(all_green_measures_df, job_id_col=job_id_col)

    all_green_measures_df = add_locations(all_green_measures_df, job_id_col=job_id_col)

    all_green_measures_df = add_sic_info(all_green_measures_df)

    soc_descriptions_dict = get_soc_info()

    all_green_measures_df = add_green_topics(all_green_measures_df)

    return all_green_measures_df, soc_descriptions_dict


def filter_large_occs(
    all_green_measures_df, min_num_job_ads=50, occ_col="SOC_2020_name"
):
    # get occupations for which we have over 50 job adverts for
    representative_occs = (
        all_green_measures_df.groupby(occ_col)
        .job_id.count()
        .sort_values(ascending=False)
        .where(lambda x: x >= min_num_job_ads)
        .dropna()
        .keys()
        .tolist()
    )

    filtered_df = all_green_measures_df[
        all_green_measures_df[occ_col].isin(representative_occs)
    ].reset_index(drop=True)

    print(f"Filtered from {len(all_green_measures_df)} rows to {len(filtered_df)}")

    return filtered_df


def create_green_skills_df(all_green_measures_df, occ_col="SOC_2020_name"):
    all_green_measures_df["ENTS_GREEN_ENTS"] = all_green_measures_df.apply(
        lambda x: x["ENTS"] + x["GREEN_ENTS"], axis=1
    )

    green_skills_df = (
        all_green_measures_df[["job_id", occ_col, "ENTS_GREEN_ENTS"]]
        .explode("ENTS_GREEN_ENTS")
        .reset_index(drop=True)
    )
    green_skills_df["ENTS_GREEN_ENTS"] = green_skills_df["ENTS_GREEN_ENTS"].apply(
        merge_ents
    )

    green_skills_df["extracted_skill"] = green_skills_df["ENTS_GREEN_ENTS"].apply(
        lambda x: x[0] if isinstance(x, list) else None
    )

    green_skills_df["green_label"] = green_skills_df["ENTS_GREEN_ENTS"].apply(
        lambda x: x[1] if isinstance(x, list) and len(x) > 4 else "not_green"
    )

    green_skills_df["green_label_prob"] = green_skills_df["ENTS_GREEN_ENTS"].apply(
        lambda x: x[2] if isinstance(x, list) and len(x) > 4 else None
    )

    green_skills_df["skill_label"] = green_skills_df["ENTS_GREEN_ENTS"].apply(
        lambda x: x[3] if isinstance(x, list) and len(x) > 4 else None
    )

    green_skills_df["skill_id"] = green_skills_df["ENTS_GREEN_ENTS"].apply(
        lambda x: x[4] if isinstance(x, list) and len(x) > 4 else None
    )

    green_skills_df = green_skills_df[green_skills_df["skill_label"] != ""]

    # # # Remove the duplicate green skills per job advert
    print(f"{len(green_skills_df)} rows deduplicated to ...")
    green_skills_df.sort_values(by="extracted_skill", inplace=True)
    green_skills_df.drop_duplicates(
        subset=["job_id", "skill_label"], keep="first", inplace=True
    )
    green_skills_df = green_skills_df[~green_skills_df["extracted_skill"].isna()]
    print(f"... {len(green_skills_df)} rows")

    return green_skills_df


def create_skill_df(skill_measures_df, job_id_col="job_id"):
    full_skills_outputs = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/green_skill_lists/20230914/full_esco_skill_mappings.json",
    )

    ents_explode = (
        skill_measures_df[[job_id_col, "ENTS"]].explode("ENTS").reset_index(drop=True)
    )
    ents_explode["skill_label"] = ents_explode["ENTS"].apply(
        lambda x: x[0] if x else []
    )
    ents_explode = ents_explode.explode("skill_label").reset_index(drop=True)

    skill_match_thresh = 0.7
    extracted_full_skill = []
    extracted_full_skill_id = []
    for skill_label in tqdm(ents_explode["skill_label"]):
        full_skills_output = full_skills_outputs.get(skill_label)
        if full_skills_output and full_skills_output[2] >= skill_match_thresh:
            extracted_full_skill.append(full_skills_output[0])
            extracted_full_skill_id.append(full_skills_output[1])
        else:
            extracted_full_skill.append(None)
            extracted_full_skill_id.append(None)

    ents_explode["extracted_full_skill"] = extracted_full_skill
    ents_explode["extracted_full_skill_id"] = extracted_full_skill_id

    green_ents_explode = (
        skill_measures_df[[job_id_col, "GREEN_ENTS"]]
        .explode("GREEN_ENTS")
        .reset_index(drop=True)
    )
    green_ents_explode["skill_label"] = green_ents_explode["GREEN_ENTS"].apply(
        lambda x: x[0] if isinstance(x, list) else None
    )
    green_ents_explode["extracted_green_skill"] = green_ents_explode[
        "GREEN_ENTS"
    ].apply(lambda x: x[1][2][0] if isinstance(x, list) else None)
    green_ents_explode["extracted_green_skill_id"] = green_ents_explode[
        "GREEN_ENTS"
    ].apply(lambda x: x[1][2][1] if isinstance(x, list) else None)

    green_skills_df = pd.concat([ents_explode, green_ents_explode])
    green_skills_df = green_skills_df[
        (
            (green_skills_df["skill_label"] != "")
            & (pd.notnull(green_skills_df["skill_label"]))
        )
    ]

    # # Remove the duplicate green skills per job advert
    green_skills_df.sort_values(by="extracted_green_skill", inplace=True)
    green_skills_df.drop_duplicates(
        subset=[job_id_col, "skill_label"], keep="first", inplace=True
    )

    green_esco_taxonomy = load_s3_data(
        BUCKET_NAME, "outputs/data/green_skill_lists/green_esco_data_formatted.csv"
    )

    full_esco_taxonomy = load_s3_data(
        "open-jobs-lake",
        "escoe_extension/outputs/data/skill_ner_mapping/esco_data_formatted.csv",
    )

    green_order_dict = {0: "preferredLabel", 1: "altLabels"}
    green_esco_taxonomy["type_sort"] = green_esco_taxonomy["type"].map(green_order_dict)
    green_esco_taxonomy.sort_values(by="type_sort", inplace=True, ascending=False)
    green_esco_taxonomy.drop_duplicates(subset=["id"], keep="first", inplace=True)
    green_skill_id_2_name = dict(
        zip(green_esco_taxonomy["id"], green_esco_taxonomy["description"])
    )

    full_order_dict = {0: "preferredLabel", 1: "altLabels", 2: "level_2", 3: "level_3"}
    full_esco_taxonomy["type_sort"] = full_esco_taxonomy["type"].map(green_order_dict)
    full_esco_taxonomy.sort_values(by="type_sort", inplace=True, ascending=False)
    full_esco_taxonomy.drop_duplicates(subset=["id"], keep="first", inplace=True)
    full_skill_id_2_name = dict(
        zip(full_esco_taxonomy["id"], full_esco_taxonomy["description"])
    )

    green_skills_df["full_skill_preferred_name"] = green_skills_df[
        "extracted_full_skill_id"
    ].map(full_skill_id_2_name)
    green_skills_df["green_skill_preferred_name"] = green_skills_df[
        "extracted_green_skill_id"
    ].map(green_skill_id_2_name)

    return green_skills_df


def create_agg_measures_per_occ(all_green_measures_df, occ_col="SOC_2020_name"):
    # generate a dataframe with summed green measures per occupation

    all_green_measures_df_ents = all_green_measures_df[
        ~all_green_measures_df["GREEN_ENTS"].isna()
    ]
    all_green_measures_df_ents["GREEN_ENTS_COUNT"] = all_green_measures_df_ents[
        "GREEN_ENTS"
    ].apply(lambda x: len(x))

    all_green_measures_df_occ = (
        all_green_measures_df_ents.groupby(occ_col)
        .aggregate(
            {
                "INDUSTRY TOTAL GHG EMISSIONS": ["mean"],
                "INDUSTRY CARBON DIOXIDE EMISSIONS PER EMPLOYEE": ["mean"],
                "GREEN TIMESHARE": ["mean"],
                "GREEN_ENTS_COUNT": ["mean"],
                "PROP_GREEN": ["mean"],
            }
        )
        .reset_index()
    )
    all_green_measures_df_occ.columns = all_green_measures_df_occ.columns.levels[0]
    all_green_measures_df_occ.columns = [
        occ_col,
        "industry_ghg_emissions_mean",
        "industry_carbon_emissions_employee_mean",
        "occupation_green_timeshare_mean",
        "green_skills_count_mean",
        "green_skill_percentage_mean",
    ]

    # pick majority occupation greenness
    occ_green_cat = all_green_measures_df.groupby(occ_col)["GREEN CATEGORY"].agg(
        lambda x: pd.Series.mode(x)[0]
    )
    # pick majority green/non-green occupation
    occ_green_nongreen = all_green_measures_df.groupby(occ_col)["GREEN/NOT GREEN"].agg(
        lambda x: pd.Series.mode(x)[0]
    )
    all_green_measures_df_occ["occ_green_non_green"] = all_green_measures_df_occ[
        occ_col
    ].map(occ_green_nongreen)
    all_green_measures_df_occ["occ_green_category"] = all_green_measures_df_occ[
        occ_col
    ].map(occ_green_cat)

    return all_green_measures_df_occ


def create_agg_data(
    all_green_measures_df,
    green_skills_df,
    soc_descriptions_dict=None,
    agg_col="SOC_2020_EXT",
    job_id_col="job_id",
):
    """
    Much like create_agg_occ_measures but more generic to aggregate by any column
    """
    aggregated_data = {}
    for agg_value in tqdm(all_green_measures_df[agg_col].unique()):
        if pd.notnull(agg_value):
            filtered_data = all_green_measures_df[
                all_green_measures_df[agg_col] == agg_value
            ]

            filtered_skills = green_skills_df[
                green_skills_df[job_id_col].isin(
                    set(filtered_data[job_id_col].tolist())
                )
            ]

            top_green_skill_num = (
                filtered_skills[
                    ["green_skill_preferred_name", "extracted_green_skill_id"]
                ]
                .value_counts()[0:5]
                .to_dict()
            )
            top_green_skill_prop = (
                filtered_skills[
                    ["green_skill_preferred_name", "extracted_green_skill_id"]
                ]
                .value_counts(normalize=True)[0:5]
                .to_dict()
            )

            green_skill_info = []
            for k, v in top_green_skill_num.items():
                green_skill_info.append(
                    {
                        "skill_name": k[0],
                        "skill_id": k[1],
                        "num_job_ads": v,
                        "prop_job_ads": top_green_skill_prop[k],
                    }
                )

            top_5_not_green_skills_num = (
                filtered_skills[pd.isnull(filtered_skills["extracted_green_skill"])][
                    ["full_skill_preferred_name", "extracted_full_skill_id"]
                ]
                .value_counts()[0:5]
                .to_dict()
            )
            top_5_not_green_skills_prop = (
                filtered_skills[pd.isnull(filtered_skills["extracted_green_skill"])][
                    ["full_skill_preferred_name", "extracted_full_skill_id"]
                ]
                .value_counts(normalize=True)[0:5]
                .to_dict()
            )

            not_green_skill_info = []
            for k, v in top_5_not_green_skills_num.items():
                not_green_skill_info.append(
                    {
                        "skill_name": k[0],
                        "skill_id": k[1],
                        "num_job_ads": v,
                        "prop_job_ads": top_5_not_green_skills_prop[k],
                    }
                )

            top_5_sics_num = (
                filtered_data[["SIC_2_digit", "SIC_2_digit_name"]]
                .value_counts()[0:5]
                .to_dict()
            )
            top_5_sics_prop = (
                filtered_data[["SIC_2_digit", "SIC_2_digit_name"]]
                .value_counts(normalize=True)[0:5]
                .to_dict()
            )

            top_5_sics = []
            for k, v in top_5_sics_num.items():
                top_5_sics.append(
                    {
                        "sic_id": k[0],
                        "sic_name": k[1],
                        "num_job_ads": v,
                        "prop_job_ads": top_5_sics_prop[k],
                    }
                )

            top_5_socs_num = (
                filtered_data[["SOC_2020_EXT", "SOC_2020_EXT_name"]]
                .value_counts()[0:5]
                .to_dict()
            )
            top_5_socs_prop = (
                filtered_data[["SOC_2020_EXT", "SOC_2020_EXT_name"]]
                .value_counts(normalize=True)[0:5]
                .to_dict()
            )

            top_5_socs = []
            for k, v in top_5_socs_num.items():
                top_5_socs.append(
                    {
                        "soc_id": k[0],
                        "soc_name": k[1],
                        "num_job_ads": v,
                        "prop_job_ads": top_5_socs_prop[k],
                    }
                )

            top_5_itl2 = filtered_data["itl_2_name"].value_counts()[0:5].to_dict()

            loc_info = {
                loc: round((num_job_ads / len(filtered_data)) * 100, 2)
                for loc, num_job_ads in top_5_itl2.items()
            }

            aggregated_data[agg_value] = {
                # General
                agg_col: agg_value,
                "num_job_ads": len(filtered_data),
                "prop_job_ads": len(filtered_data) / len(all_green_measures_df),
                # Occupations
                "top_5_socs": top_5_socs,
                "occ_timeshare": filtered_data["GREEN TIMESHARE"].mode()[0]
                if len(filtered_data["GREEN TIMESHARE"].mode()) != 0
                else None,
                "occ_topics": filtered_data["GREEN TOPICS"].mode()[0]
                if len(filtered_data["GREEN TOPICS"].mode()) != 0
                else None,
                "average_occ_green_timeshare": filtered_data["GREEN TIMESHARE"].mean(),
                # Skills
                "average_num_skills": filtered_data["NUM_SPLIT_ENTS"].mean(),
                "average_prop_green_skills": filtered_data["PROP_GREEN"].mean(),
                # 'var_prop_green_skills': occ_filtered_data['PROP_GREEN'].var(skipna=True),
                "top_5_green_skills": green_skill_info,
                "top_5_not_green_skills": not_green_skill_info,
                # Industry
                "num_unique_SIC2": filtered_data["SIC_2_digit"].nunique(),
                "num_null_sic2": len(
                    filtered_data[pd.isnull(filtered_data["SIC_2_digit"])]
                ),
                "num_top_sic2": filtered_data["SIC_2_digit"].value_counts()[0]
                if len(filtered_data["SIC_2_digit"].value_counts()) > 0
                else None,
                "num_other_sic2": sum(filtered_data["SIC_2_digit"].value_counts()[1:])
                if len(filtered_data["SIC_2_digit"].value_counts()) > 1
                else None,
                "average_ind_perunit_ghg": filtered_data[
                    "INDUSTRY GHG PER UNIT EMISSIONS"
                ].mean(),
                "average_ind_prop_hours": filtered_data[
                    "INDUSTRY PROP HOURS GREEN TASKS"
                ].mean(),
                "average_ind_prop_workers": filtered_data[
                    "INDUSTRY PROP WORKERS GREEN TASKS"
                ].mean(),
                "top_5_sics": top_5_sics,
                # metadata
                ##salary information
                "median_min_annualised_salary": filtered_data.min_annualised_salary.median(),
                "median_max_annualised_salary": filtered_data.max_annualised_salary.median(),
                ##location information
                "top_5_itl2_prop": [loc_info],
            }

            if agg_col == "SOC_2020_EXT":
                SOC_2020_EXT_name = (
                    filtered_data["SOC_2020_EXT_name"].mode()[0]
                    if len(filtered_data["SOC_2020_EXT_name"].mode()) != 0
                    else None
                )
                soc_desc = soc_descriptions_dict.get(agg_value, None)
                soc_name_cleaned = clean_soc_name(SOC_2020_EXT_name)

                aggregated_data[agg_value].update(
                    {
                        "SOC_2020_EXT": agg_value,
                        "SOC_2020_EXT_name": SOC_2020_EXT_name,
                        "clean_soc_name": soc_name_cleaned,
                        "soc_description": soc_desc,
                        "SOC_2020": filtered_data["SOC_2020"].mode()[0]
                        if len(filtered_data["SOC_2020"].mode()) != 0
                        else None,
                        "SOC_2010": filtered_data["SOC_2010"].mode()[0]
                        if len(filtered_data["SOC_2010"].mode()) != 0
                        else None,
                    }
                )

            if "green_topics_lists" in filtered_data:
                aggregated_data[agg_value].update(
                    {"green_topics_lists": filtered_data["green_topics_lists"].iloc[0]}
                )

    aggregated_data = pd.DataFrame(aggregated_data).T
    aggregated_data = aggregated_data.reset_index()
    aggregated_data.rename(columns={"index": agg_col}, inplace=True)
    return aggregated_data


def categorical_assign(value, all_values, rev=False):
    q1 = all_values.quantile(0.33)
    q2 = all_values.quantile(0.66)

    if pd.notnull(value):
        if value <= q1:
            if rev:
                return "high"
            else:
                return "low"
        elif value <= q2:
            return "mid"
        else:
            if rev:
                return "low"
            else:
                return "high"
    else:
        return None


def get_one_score(occ, ind, skill):
    score_dict = {"high": 2, "mid": 1, "low": 0}
    if occ in score_dict:
        if ind in score_dict:
            score = score_dict[occ] + score_dict[ind] + score_dict[skill]
            # return score
            if score <= 1:
                # 0,1
                return "low"
            elif score <= 3:
                # 2,3
                return "low-mid"
            elif score <= 5:
                # 4,5
                return "mid-high"
            else:
                # 6
                return "high"
        else:
            None
    else:
        return None


def get_overall_greenness(occ_aggregated_df):
    occ_aggregated_df["occ_greenness"] = occ_aggregated_df["occ_timeshare"].apply(
        lambda x: categorical_assign(x, occ_aggregated_df["occ_timeshare"])
    )
    occ_aggregated_df["ind_greenness"] = occ_aggregated_df[
        "average_ind_perunit_ghg"
    ].apply(
        lambda x: categorical_assign(
            x, occ_aggregated_df["average_ind_perunit_ghg"], rev=True
        )
    )

    occ_aggregated_df["skills_greenness"] = occ_aggregated_df[
        "average_prop_green_skills"
    ].apply(
        lambda x: categorical_assign(x, occ_aggregated_df["average_prop_green_skills"])
    )

    occ_aggregated_df["greenness_score"] = occ_aggregated_df.apply(
        lambda x: get_one_score(
            x["occ_greenness"], x["ind_greenness"], x["skills_greenness"]
        ),
        axis=1,
    )

    return occ_aggregated_df
