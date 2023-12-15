"""
Load the green measures from a sample of OJO data and process it into a form needed to create analysis from
"""

from dap_prinz_green_jobs import BUCKET_NAME, logger, analysis_config
from dap_prinz_green_jobs.getters.data_getters import load_s3_data
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
        return soc_name.replace("n.e.c.", "").strip()
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
    green_inds_outputs = green_inds_outputs[
        green_inds_outputs["INDUSTRY GHG PER UNIT EMISSIONS"] != ":"
    ].reset_index(drop=True)
    green_inds_outputs["INDUSTRY TOTAL GHG EMISSIONS"] = green_inds_outputs[
        "INDUSTRY TOTAL GHG EMISSIONS"
    ].apply(lambda x: float(x) if x != "" else np.nan)
    green_inds_outputs["INDUSTRY GHG PER UNIT EMISSIONS"] = green_inds_outputs[
        "INDUSTRY GHG PER UNIT EMISSIONS"
    ].apply(lambda x: float(x) if x != "" else np.nan)

    green_inds_outputs["INDUSTRY PROP HOURS GREEN TASKS"] = green_inds_outputs[
        "INDUSTRY PROP HOURS GREEN TASKS"
    ].apply(lambda x: float(x) if x != "" else np.nan)
    green_inds_outputs["INDUSTRY GHG EMISSIONS PER EMPLOYEE"] = green_inds_outputs[
        "INDUSTRY GHG EMISSIONS PER EMPLOYEE"
    ].apply(lambda x: float(x) if x != "" else np.nan)
    green_inds_outputs[
        "INDUSTRY CARBON DIOXIDE EMISSIONS PER EMPLOYEE"
    ] = green_inds_outputs["INDUSTRY CARBON DIOXIDE EMISSIONS PER EMPLOYEE"].apply(
        lambda x: float(x) if x != "" else np.nan
    )

    return green_inds_outputs


def load_ojo_green_measures(
    analysis_config: Dict[str, str] = analysis_config
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, str]]:
    """Loads and cleans the green measures for skills, occupations and industries.

    Args:
        analysis_config (Dict[str, str], optional): Analysis config dictionary. Defaults to analysis_config.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, str]]: Tuple of dataframes for skills, occupations and industries green measures and a dictionary of soc codes and names.
    """
    green_skills_outputs = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/ojo_application/extracted_green_measures/{analysis_config['skills_date_stamp']}/ojo_large_sample_skills_green_measures_production_{analysis_config['production']}.csv",
    )
    green_skills_outputs["GREEN_ENTS"] = green_skills_outputs["GREEN_ENTS"].apply(
        safe_literal_eval
    )
    green_skills_outputs["ENTS"] = green_skills_outputs["ENTS"].apply(safe_literal_eval)

    green_occs_outputs = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/ojo_application/extracted_green_measures/{analysis_config['occ_date_stamp']}/ojo_large_sample_occupation_green_measures_production_{analysis_config['production'].lower()}.csv",
    )
    green_occs_outputs = process_soc_columns(green_occs_outputs)

    soc_name_dict = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/ojo_application/extracted_green_measures/{analysis_config['occ_date_stamp']}/soc_name_dict.json",
    )
    green_inds_outputs = load_s3_data(
        BUCKET_NAME,
        f"outputs/data/ojo_application/extracted_green_measures/{analysis_config['ind_date_stamp']}/ojo_large_sample_industry_green_measures_production_{analysis_config['production']}.csv",
    )

    green_inds_outputs = process_ind_columns(green_inds_outputs)

    return green_skills_outputs, green_occs_outputs, green_inds_outputs, soc_name_dict


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


def merge_green_measures(
    skill_measures_df: pd.DataFrame,
    occs_measures_df: pd.DataFrame,
    inds_measures_df: pd.DataFrame,
    soc_name_dict: dict,
    job_id_col: str = "job_id",
) -> pd.DataFrame:
    """
    Merge all 3 green measures into one dataframe where each row is a job advert.
    """
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

    print(f"Filtered from {len(all_green_measures_df)} rows to {len(filtered_df)}")

    return filtered_df


def create_skill_df(
    skill_measures_df: pd.DataFrame,
    job_id_col: str = "job_id",
    skill_match_thresh: float = 0.7,
) -> pd.DataFrame:
    """
    Process the skills measures dataframe where each row is a job advert, into a format
    where each row is a skill and there is information about which job advert it was found in,
    whether it is green or not, and which esco skill it maps to.
    """
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


def create_agg_measures_per_occ(
    all_green_measures_df: pd.DataFrame, occ_col: str = "SOC_2020_name"
) -> pd.DataFrame:
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
    all_green_measures_df: pd.DataFrame,
    green_skills_df: pd.DataFrame,
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


def categorical_assign(
    value, all_values: pd.Series, rev: bool = False
) -> Union[str, None]:
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


def get_one_score(occ: str, ind: str, skill: str) -> Union[str, None]:
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


def get_overall_greenness(occ_aggregated_df: pd.DataFrame) -> pd.DataFrame:
    """
    Find where in the distribution of green measures for all occupations this
    occupation sits, for each of the 3 measures plus a combined score.
    e.g. data scientists are in the highest 1/3 of all industry greenness measures.
    """
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
