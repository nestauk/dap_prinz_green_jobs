"""
Load the green measures from a sample of OJO data and process it into a form needed to create analysis from
"""

from dap_prinz_green_jobs import BUCKET_NAME, logger, analysis_config
from dap_prinz_green_jobs.getters.data_getters import load_s3_data, get_s3_data_paths
from dap_prinz_green_jobs.getters.industry_getters import load_sic
from dap_prinz_green_jobs.getters.occupation_getters import load_soc_descriptions
from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupations_measures_utils import (
    OccupationMeasures,
)

import pandas as pd
import numpy as np
from tqdm import tqdm

from typing import Tuple, Dict, Union
import ast
import re


def get_mode(series: pd.Series) -> str:
    """Get mode of a series.

    Args:
        series (pd.Series): Series to get mode of.

    Returns:
        str: Mode of series.
    """
    return series.value_counts().index[0]


# clean up skills
def merge_ents(ents):
    """Merge entities.

    Args:
        ents (_type_): Entity list.

    Returns:
        Merged entities.
    """
    if not isinstance(ents, list):
        return None

    elif "green" in ents[1]:
        return [ents[0]] + [ents[1][0]] + [ents[1][1]] + ents[1][2]
    else:
        return ents[0] + [ents[1]]


def clean_soc_name(soc_name: Union[str, None]) -> Union[str, None]:
    """Cleans SOC name to:
        - replace n.e.c. with nothing
        - strip whitespace

    Args:
        soc_name (Union[str, None]): SOC name

    Returns:
        Union[str, None]: Cleaned SOC name
    """
    if soc_name:
        return soc_name.replace("n.e.c.", "").replace("n.e.c", "").strip()
    else:
        return None


def clean_sic_name(sic_name: Union[str, None]) -> Union[str, None]:
    """Cleans SIC name to:
        - replace 'nec' or ', nec' with nothing
        - strip whitespace

    Args:
        sic_name (Union[str, None]): SIC name

    Returns:
        Union[str, None]: Cleaned SIC name
    """
    if pd.notnull(sic_name):
        pattern = re.compile(r"(,)?( )?nec$")
        return re.sub(pattern, "", sic_name)
    else:
        return None


def process_soc_columns(
    occs_measures_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    The SOC column is originally a string of a dictionary of SOC details,
    format this and move the information into separate columns
    """

    # "SOC" is read as a string, but "{... 'name': nan}" causes issues with literal_eval
    occs_measures_df["SOC"] = occs_measures_df["SOC"].apply(
        lambda x: ast.literal_eval(x.replace("'name': nan", "'name': 'None'"))
        if pd.notnull(x)
        else None
    )

    # Separate out the SOC columns
    for soc_columns in ["SOC_2020_EXT", "SOC_2020", "SOC_2010", "name"]:
        occs_measures_df[soc_columns] = occs_measures_df["SOC"].apply(
            lambda x: x[soc_columns] if (x and x != "None") else None
        )

    occs_measures_df["GREEN TIMESHARE"] = occs_measures_df["GREEN TIMESHARE"].apply(
        lambda x: float(x) if x != "" else np.nan
    )

    return occs_measures_df


def process_ind_columns(
    green_inds_outputs: pd.DataFrame,
) -> pd.DataFrame:
    """
    Format the industry columns
    """
    green_inds_outputs = green_inds_outputs.fillna(value=np.nan)
    green_inds_outputs = green_inds_outputs[
        green_inds_outputs["INDUSTRY GHG PER UNIT EMISSIONS"] != ":"
    ].reset_index(drop=True)
    green_inds_outputs["INDUSTRY TOTAL GHG EMISSIONS"] = green_inds_outputs[
        "INDUSTRY TOTAL GHG EMISSIONS"
    ].apply(lambda x: float(x) if ((x != "") & (x != None)) else np.nan)
    green_inds_outputs["INDUSTRY GHG PER UNIT EMISSIONS"] = green_inds_outputs[
        "INDUSTRY GHG PER UNIT EMISSIONS"
    ].apply(lambda x: float(x) if ((x != "") & (x != None) & (x != "None")) else np.nan)
    green_inds_outputs["INDUSTRY PROP HOURS GREEN TASKS"] = green_inds_outputs[
        "INDUSTRY PROP HOURS GREEN TASKS"
    ].apply(lambda x: float(x) if ((x != "") & (x != None)) else np.nan)
    green_inds_outputs["INDUSTRY GHG EMISSIONS PER EMPLOYEE"] = green_inds_outputs[
        "INDUSTRY GHG EMISSIONS PER EMPLOYEE"
    ].apply(lambda x: float(x) if ((x != "") & (x != None)) else np.nan)
    green_inds_outputs[
        "INDUSTRY CARBON DIOXIDE EMISSIONS PER EMPLOYEE"
    ] = green_inds_outputs["INDUSTRY CARBON DIOXIDE EMISSIONS PER EMPLOYEE"].apply(
        lambda x: float(x) if ((x != "") & (x != None)) else np.nan
    )
    # Clean SIC name (remove 'nec')
    green_inds_outputs["SIC_name"] = green_inds_outputs["SIC_name"].apply(
        clean_sic_name
    )
    green_inds_outputs["job_id"] = green_inds_outputs["job_id"].astype("int64")
    return green_inds_outputs


def safe_literal_eval(value) -> Union[None, str, int, float, list, dict]:
    """
    Safely evaluate an expression node or a string containing a Python literal or container display.
    """
    try:
        return ast.literal_eval(value)
    except (SyntaxError, ValueError):
        # Handle the exception (e.g., return a default value or NaN)
        return None


def convert_green_ents(ents):
    """Convert green entities.

    Args:
        ents (_type_): Entity list.

    Returns:
        _type_: Converted entities.
    """

    if isinstance(ents, list):
        new_ents = []
        for sublist in ents:
            if not isinstance(sublist[0], list):
                sublist[0] = [sublist[0]]
            new_ents.append(sublist)

        return new_ents
    else:
        return ents


def add_salaries(
    salary_information: pd.DataFrame,
    all_green_measures_df: pd.DataFrame,
    job_id_col: str = "job_id",
) -> pd.DataFrame:
    salary_information[job_id_col] = salary_information.id

    # add salary
    all_green_measures_df = pd.merge(
        all_green_measures_df, salary_information, on=job_id_col, how="left"
    )
    return all_green_measures_df


def add_locations(
    locations_information: pd.DataFrame,
    all_green_measures_df: pd.DataFrame,
    job_id_col: str = "job_id",
) -> pd.DataFrame:
    locations_information[job_id_col] = locations_information.id

    # add locations
    locations_information = locations_information.drop(
        columns=["is_uk", "is_large_geo", "location", "coordinates"]
    )
    all_green_measures_df = pd.merge(
        all_green_measures_df, locations_information, on=job_id_col, how="left"
    )
    return all_green_measures_df


def add_sic_info(all_green_measures_df: pd.DataFrame) -> pd.DataFrame:
    sic_data = load_sic()
    sic_data = sic_data[sic_data["Level headings"] == "Division"]
    sic_names = dict(
        zip(sic_data["Division"].tolist(), sic_data["Description"].tolist())
    )

    # Add these new columns
    all_green_measures_df["SIC_2_digit"] = all_green_measures_df["SIC"].apply(
        lambda x: str(x)[0:2] if pd.notnull(x) else None
    )
    all_green_measures_df["SIC_2_digit_name"] = all_green_measures_df[
        "SIC_2_digit"
    ].apply(lambda x: sic_names.get(x) if pd.notnull(x) else None)

    return all_green_measures_df


def get_soc_info() -> dict:
    # we need soc descriptions as well - they have descriptions for 6 digit sic codes
    soc_descriptions = load_soc_descriptions()

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


def add_green_topics(all_green_measures_df: pd.DataFrame) -> pd.DataFrame:
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


def add_additional_metadata(
    all_green_measures_df: pd.DataFrame,
    salary_information: pd.DataFrame,
    locations_information: pd.DataFrame,
    job_id_col: str = "job_id",
) -> tuple:
    all_green_measures_df = add_salaries(
        salary_information, all_green_measures_df, job_id_col=job_id_col
    )

    all_green_measures_df = add_locations(
        locations_information, all_green_measures_df, job_id_col=job_id_col
    )

    all_green_measures_df = add_sic_info(all_green_measures_df)

    soc_descriptions_dict = get_soc_info()

    all_green_measures_df = add_green_topics(all_green_measures_df)

    return all_green_measures_df, soc_descriptions_dict


def filter_large_occs(
    all_green_measures_df: pd.DataFrame,
    min_num_job_ads: int = 50,
    occ_col: str = "SOC_2020_name",
) -> pd.DataFrame:
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

    logger.info(
        f"Filtered from {len(all_green_measures_df)} rows to {len(filtered_df)}"
    )

    return filtered_df


def read_process_taxonomies():
    logger.info("Loading skills taxonomies")
    green_esco_taxonomy = load_s3_data(
        BUCKET_NAME,
        "outputs/data/green_skill_lists/green_esco_data_formatted_20231129.csv",
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
    full_esco_taxonomy["type_sort"] = full_esco_taxonomy["type"].map(full_order_dict)
    full_esco_taxonomy.sort_values(by="type_sort", inplace=True, ascending=False)
    full_esco_taxonomy.drop_duplicates(subset=["id"], keep="first", inplace=True)
    full_skill_id_2_name = dict(
        zip(full_esco_taxonomy["id"], full_esco_taxonomy["description"])
    )

    # Extras (knowledge, transversal and S1, S2 etc)
    extra_full_esco_taxonomy = load_s3_data(
        "open-jobs-lake",
        "escoe_extension/inputs/data/esco/skillGroups_en.csv",
    )
    highest_level_map = dict(
        zip(
            extra_full_esco_taxonomy["code"], extra_full_esco_taxonomy["preferredLabel"]
        )
    )
    full_skill_id_2_name.update(highest_level_map)

    transversal_name_mapper = load_s3_data(
        "open-jobs-lake",
        "escoe_extension/inputs/data/esco/esco_transversal_mapper.json",
    )
    full_skill_id_2_name.update(transversal_name_mapper)

    knowledge_taxonomy = load_s3_data(
        "open-jobs-lake",
        "escoe_extension/inputs/data/esco/skillsHierarchy_en.csv",
    )

    knowledge_taxonomy = knowledge_taxonomy[
        knowledge_taxonomy["Level 0 preferred term"] == "knowledge"
    ]
    knowledge_taxonomy["code"] = knowledge_taxonomy["Level 1 URI"].apply(
        lambda x: "K" + str(x).split("/")[-1] if pd.notnull(x) else None
    )
    knowledge_mapper = dict(
        zip(knowledge_taxonomy["code"], knowledge_taxonomy["Level 1 preferred term"])
    )

    full_skill_id_2_name.update(knowledge_mapper)

    return green_skill_id_2_name, full_skill_id_2_name


def create_agg_data(
    all_green_measures_df: pd.DataFrame,
    green_skills_df: pd.DataFrame,
    green_skill_id_2_name: dict,
    full_skill_id_2_name: dict,
    soc_descriptions_dict: Union[dict, None] = None,
    agg_col: str = "SOC_2020_EXT",
    job_id_col: str = "job_id",
) -> pd.DataFrame:
    """
    Much like create_agg_occ_measures but more generic to aggregate by any column
    """
    prop_job_ads_per_itl2_all = (
        all_green_measures_df["itl_2_name"].value_counts(normalize=True).to_dict()
    )

    aggregated_data = {}
    for agg_value, filtered_data in tqdm(all_green_measures_df.groupby(agg_col)):
        if pd.notnull(agg_value):
            filtered_skills = green_skills_df[
                green_skills_df[job_id_col].isin(
                    set(filtered_data[job_id_col].tolist())
                )
            ]

            top_green_skill_num = (
                filtered_skills["extracted_green_skill_id"]
                .value_counts()[0:5]
                .to_dict()
            )
            top_green_skill_prop = (
                filtered_skills["extracted_green_skill_id"]
                .value_counts(normalize=True)[0:5]
                .to_dict()
            )

            green_skill_info = []
            for k, v in top_green_skill_num.items():
                green_skill_info.append(
                    {
                        "skill_name": green_skill_id_2_name.get(k),
                        "skill_id": k,
                        "num_job_ads": v,
                        "prop_job_ads": top_green_skill_prop[k],
                    }
                )

            top_5_not_green_skills_num = (
                filtered_skills[pd.isnull(filtered_skills["extracted_green_skill_id"])][
                    "extracted_full_skill_id"
                ]
                .value_counts()[0:5]
                .to_dict()
            )
            top_5_not_green_skills_prop = (
                filtered_skills[pd.isnull(filtered_skills["extracted_green_skill_id"])][
                    "extracted_full_skill_id"
                ]
                .value_counts(normalize=True)[0:5]
                .to_dict()
            )

            not_green_skill_info = []
            for k, v in top_5_not_green_skills_num.items():
                not_green_skill_info.append(
                    {
                        "skill_name": full_skill_id_2_name.get(k),
                        "skill_id": k,
                        "num_job_ads": v,
                        "prop_job_ads": top_5_not_green_skills_prop[k],
                    }
                )

            top_5_sics_num = (
                filtered_data[["SIC", "SIC_name"]].value_counts()[0:5].to_dict()
            )
            top_5_sics_prop = (
                filtered_data[["SIC", "SIC_name"]]
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

            # get the location quotient for the aggregate data, to find the top 5 regions which
            # have above average proportions of job adverts for this aggregate (e.g. occupation)
            prop_job_ads_per_itl2_filt = (
                filtered_data["itl_2_name"].value_counts(normalize=True).to_dict()
            )

            agg_quotient = {
                k: v / prop_job_ads_per_itl2_all[k]
                for k, v in prop_job_ads_per_itl2_filt.items()
            }
            # scores above 1 are higher than normal
            loc_info = {
                k: round(v, 2)
                for k, v in sorted(
                    agg_quotient.items(), key=lambda item: item[1], reverse=True
                )[0:5]
                if v >= 1
            }

            # averaged green measures per year
            yearly_data = (
                filtered_data.groupby("year")
                .agg(
                    {
                        "job_id": "nunique",
                        "PROP_GREEN": "mean",
                        "INDUSTRY GHG PER UNIT EMISSIONS": "mean",
                        "GREEN TIMESHARE": "mean",
                    }
                )
                .rename(
                    columns={
                        "job_id": "num_job_ads",
                        "PROP_GREEN": "av_prop_green_skills",
                        "INDUSTRY GHG PER UNIT EMISSIONS": "av_ind_perunit_ghg",
                        "GREEN TIMESHARE": "av_occ_green_timeshare",
                    }
                )
                .round(4)
                .reset_index()
                .to_dict(orient="records")
            )
            yearly_data = [y for y in yearly_data if y["year"] != 2020]

            aggregated_data[agg_value] = {
                # General
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
                "num_unique_SIC": filtered_data["SIC"].nunique(),
                "num_null_sic": len(filtered_data[pd.isnull(filtered_data["SIC"])]),
                "num_top_sic": filtered_data["SIC"].value_counts()[0]
                if len(filtered_data["SIC"].value_counts()) > 0
                else None,
                "num_other_sic": sum(filtered_data["SIC"].value_counts()[1:])
                if len(filtered_data["SIC"].value_counts()) > 1
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
                "top_5_itl2_quotient": [loc_info],
                "yearly_data": yearly_data,
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
            if agg_col == "SIC":
                SIC_name = (
                    filtered_data["SIC_name"].mode()[0]
                    if len(filtered_data["SIC_name"].mode()) != 0
                    else None
                )
                SIC_2_digit_name = (
                    filtered_data["SIC_2_digit_name"].mode()[0]
                    if len(filtered_data["SIC_2_digit_name"].mode()) != 0
                    else None
                )
                aggregated_data[agg_value].update(
                    {
                        "SIC_name": SIC_name,
                        "SIC_2_digit_name": SIC_2_digit_name,
                    }
                )

            if agg_col in ["itl_1_code", "itl_2_code", "itl_3_code"]:
                name_column = agg_col.replace("_code", "_name")
                itl_name = (
                    filtered_data[name_column].mode()[0]
                    if len(filtered_data[name_column].mode()) != 0
                    else None
                )
                aggregated_data[agg_value].update({name_column: itl_name})

            if "green_topics_lists" in filtered_data:
                aggregated_data[agg_value].update(
                    {"green_topics_lists": filtered_data["green_topics_lists"].iloc[0]}
                )

    aggregated_data = pd.DataFrame(aggregated_data).T
    aggregated_data = aggregated_data.reset_index()
    aggregated_data.rename(
        columns={"index": agg_col}, inplace=True
    )  # Important since this isn't added elsewhere
    return aggregated_data


def categorical_assign(
    value, all_values: pd.Series, rev: bool = False, zero_zone: float = -0.1
) -> Union[str, None]:
    all_values_no_zero = all_values[all_values > zero_zone]
    lower_fence = all_values_no_zero.min()
    upper_fence = all_values_no_zero.quantile(0.99)
    interval_width = (upper_fence - lower_fence) / 3

    if pd.notnull(value):
        if value > zero_zone:
            if value <= interval_width:
                if rev:
                    return "high"
                else:
                    return "low"
            elif value <= interval_width * 2:
                return "mid"
            else:
                if rev:
                    return "low"
                else:
                    return "high"
        else:
            return "zero"
    else:
        return None


def get_one_score(occ: str, ind: str, skill: str) -> Union[str, None]:
    score_dict = {"high": 2, "mid": 1, "low": 0, "zero": 0}
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


def get_overall_greenness(occ_aggregated_df: pd.DataFrame) -> pd.DataFrame:
    """
    Find where in the distribution of green measures for all occupations this
    occupation sits, for each of the 3 measures plus a combined score.
    e.g. data scientists are in the highest 1/3 of all industry greenness measures.
    """
    occ_aggregated_df["occ_greenness"] = occ_aggregated_df["occ_timeshare"].apply(
        lambda x: categorical_assign(x, occ_aggregated_df["occ_timeshare"], zero_zone=0)
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
        lambda x: categorical_assign(
            x, occ_aggregated_df["average_prop_green_skills"], zero_zone=0.001
        )
    )

    occ_aggregated_df["greenness_score"] = occ_aggregated_df.apply(
        lambda x: get_one_score(
            x["occ_greenness"], x["ind_greenness"], x["skills_greenness"]
        ),
        axis=1,
    )

    return occ_aggregated_df
