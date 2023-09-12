"""
Utils to match skill embeddings to all of the ESCO taxonomy. This is a temporary measure since at the moment there
is no functionality in ojd_daps_skills to take in already embedded skills and map them.

As such, very little in this script is actually new code. It is mostly all copied from ojd_daps_skills, specifically,
- ojd_daps_skills.pipeline.skill_ner_mapping.skill_ner_mapper.py
- ojd_daps_skills.config.extract_skills_esco.yaml

"""

from ojd_daps_skills.pipeline.skill_ner_mapping.skill_ner_mapper_utils import (
    get_top_comparisons,
    get_most_common_code,
)
from dap_prinz_green_jobs.getters.data_getters import save_to_s3, load_s3_data

import re
import time
import itertools
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import pandas as pd
import os

import ast


def clean_string_list(string_list):
    """
    THIS IS THE SAME AS clean_string_list() IN skill_ner_mapper.py
    """

    if pd.notnull(string_list):
        if isinstance(string_list, str):
            return ast.literal_eval(string_list)
        else:
            return string_list
    else:
        return None


def final_prediction(
    skills_to_taxonomy,
    hier_name_mapper,
    match_thresholds_dict,
    num_hier_levels,
):
    """
    THIS IS THE SAME AS final_prediction() IN skill_ner_mapper.py, just with some self. removed

    Using all the information in skill_mapper_list get a final ESCO match (if any)
    for each ojo skill, based off a set of rules.
    """

    rank_matches = []
    for match_id, v in enumerate(skills_to_taxonomy):
        match_num = 0

        # Try to find a close similarity skill
        skill_info = {
            "ojo_skill": v["ojo_ner_skill"],
            "match_id": v["ojo_skill_id"],
        }
        match_hier_info = {}
        top_skill, top_skill_code, top_sim_score = v["top_tax_skills"][0]
        if top_sim_score >= match_thresholds_dict["skill_match_thresh"]:
            skill_info.update({"match " + str(match_num): top_skill})
            match_hier_info[match_num] = {
                "match_code": top_skill_code,
                "type": "skill",
                "value": top_sim_score,
            }
            match_num += 1

        # Go through hierarchy levels from most granular to least
        # and try to find a close match first in the most common level then in
        # the level name with the closest similarity
        for n in reversed(range(num_hier_levels)):
            # Look at level n most common
            type_name = "most_common_level_" + str(n)
            if "high_tax_skills" in v.keys():
                if (type_name in v["high_tax_skills"]) and (
                    n in match_thresholds_dict["max_share"]
                ):
                    c0 = v["high_tax_skills"][type_name]
                    if (c0[1]) and (c0[1] >= match_thresholds_dict["max_share"][n]):
                        match_name = hier_name_mapper.get(c0[0], c0[0])
                        skill_info.update({"match " + str(match_num): match_name})
                        match_hier_info[match_num] = {
                            "match_code": c0[0],
                            "type": type_name,
                            "value": c0[1],
                        }
                        match_num += 1

            # Look at level n closest similarity
            type_name = "top_level_" + str(n) + "_tax_level"
            if (type_name in v) and (n in match_thresholds_dict["top_tax_skills"]):
                c1 = v[type_name]
                if c1[2] >= match_thresholds_dict["top_tax_skills"][n]:
                    skill_info.update({"match " + str(match_num): c1[0]})
                    match_hier_info[match_num] = {
                        "match_code": c1[1],
                        "type": type_name,
                        "value": c1[2],
                    }
                    match_num += 1

        skill_info.update({"match_info": match_hier_info})
        rank_matches.append(skill_info)

    # Just pull out the top matches for each ojo skill
    final_match = []
    for rank_match in rank_matches:
        if "match 0" in rank_match.keys():
            final_match.append(
                {
                    "ojo_skill": rank_match["ojo_skill"],
                    "ojo_job_skill_hash": rank_match["match_id"],
                    "match_skill": rank_match["match 0"],
                    "match_score": rank_match["match_info"][0]["value"],
                    "match_type": rank_match["match_info"][0]["type"],
                    "match_id": rank_match["match_info"][0]["match_code"],
                }
            )

    return final_match


def map_esco_skills(
    skill_ents: list, all_extracted_skills_embeddings_dict: dict
) -> dict:
    """
    Map skills to the most semantically similar ESCO skill

    This function is very similar to what is in skill_ner_mapper.py in the ojd_daps_skills package.

    Args:
        skill_ents: a list of skills
        all_extracted_skills_embeddings_dict: the associated embeddings for the skills in skill_ents

    Returns:
        dict: The skill mapped to an ESCO skill (if it does map)
    """

    # -----------------------------------------------------------------------
    # THE FOLLOWING IS NEW, BUT ALL STILL COPIED from the config file and parts of extract_skills.py
    # -----------------------------------------------------------------------

    skill_type_col = "type"
    skill_type_dict = {
        "skill_types": ["preferredLabel", "altLabels"],
        "hier_types": ["level_2", "level_3"],
    }
    skill_types = skill_type_dict.get("skill_types", [])
    skill_hashes_filtered = dict(zip(range(len(skill_ents)), skill_ents))
    skill_name_col = "description"
    skill_id_col = "id"
    skill_hier_info_col = "hierarchy_levels"
    num_hier_levels = 4
    match_thresholds_dict = {
        "skill_match_thresh": 0.7,
        "top_tax_skills": {1: 0.5, 2: 0.5, 3: 0.5},
        "max_share": {1: 0, 2: 0.2, 3: 0.2},
    }

    taxonomy_skills = load_s3_data(
        "open-jobs-lake",
        "escoe_extension/outputs/data/skill_ner_mapping/esco_data_formatted.csv",
    )

    taxonomy_skills = taxonomy_skills[
        taxonomy_skills[skill_name_col].notna()
    ].reset_index(drop=True)
    taxonomy_skills[skill_hier_info_col] = taxonomy_skills[skill_hier_info_col].apply(
        clean_string_list
    )

    saved_taxonomy_embeds = load_s3_data(
        "open-jobs-lake",
        "escoe_extension/outputs/data/skill_ner_mapping/esco_embeddings.json",
    )

    taxonomy_skills_embeddings_dict = {
        int(embed_indx): np.array(embedding)
        for embed_indx, embedding in saved_taxonomy_embeds.items()
    }

    clean_ojo_skill_embeddings = list(all_extracted_skills_embeddings_dict.values())

    # -----------------------------------------------------------------------
    # THE FOLLOWING IS COPIED FROM `skill_ner_mapper.py` (with self. removed)
    # -----------------------------------------------------------------------

    tax_skills_ix = taxonomy_skills[
        taxonomy_skills[skill_type_col].isin(skill_types)
    ].index

    (skill_top_sim_indxs, skill_top_sim_scores) = get_top_comparisons(
        clean_ojo_skill_embeddings,
        [taxonomy_skills_embeddings_dict[i] for i in tax_skills_ix],
        match_sim_thresh=0.5,
    )

    # Find the closest matches to the hierarchy levels information
    hier_types = {i: v for i, v in enumerate(skill_type_dict.get("hier_types", []))}
    hier_types_top_sims = {}
    for hier_type_num, hier_type in hier_types.items():
        taxonomy_skills_ix = taxonomy_skills[
            taxonomy_skills[skill_type_col] == hier_type
        ].index
        top_sim_indxs, top_sim_scores = get_top_comparisons(
            clean_ojo_skill_embeddings,
            [taxonomy_skills_embeddings_dict[i] for i in taxonomy_skills_ix],
        )
        hier_types_top_sims[hier_type_num] = {
            "top_sim_indxs": top_sim_indxs,
            "top_sim_scores": top_sim_scores,
            "taxonomy_skills_ix": taxonomy_skills_ix,
        }

    # Output the top matches (using the different metrics) for each OJO skill
    # Need to match indexes back correctly (hence all the ix variables)
    skill_mapper_list = []
    for i, (match_i, match_text) in enumerate(skill_hashes_filtered.items()):
        # Top highest matches (any threshold)
        match_results = {
            "ojo_skill_id": match_i,
            "ojo_ner_skill": match_text,
            "top_tax_skills": list(
                zip(
                    [
                        taxonomy_skills.iloc[tax_skills_ix[top_ix]][skill_name_col]
                        for top_ix in skill_top_sim_indxs[i]
                    ],
                    [
                        taxonomy_skills.iloc[tax_skills_ix[top_ix]][skill_id_col]
                        for top_ix in skill_top_sim_indxs[i]
                    ],
                    skill_top_sim_scores[i],
                )
            ),
        }
        # Using the top matches, find the most common codes for each level of the
        # hierarchy (if hierarchy details are given), weighted by their similarity score
        if skill_hier_info_col:
            high_hier_codes = []
            for sim_ix, sim_score in zip(
                skill_top_sim_indxs[i], skill_top_sim_scores[i]
            ):
                tax_info = taxonomy_skills.iloc[tax_skills_ix[sim_ix]]
                if tax_info[skill_hier_info_col]:
                    hier_levels = tax_info[skill_hier_info_col]
                    for hier_level in hier_levels:
                        high_hier_codes += [hier_level] * round(sim_score * 10)
            high_tax_skills_results = {}
            for hier_level in range(num_hier_levels):
                high_tax_skills_results[
                    "most_common_level_" + str(hier_level)
                ] = get_most_common_code(high_hier_codes, hier_level)
            if high_tax_skills_results:
                match_results["high_tax_skills"] = high_tax_skills_results
        # Now get the top matches using the hierarchy descriptions (if hier_types isnt empty)
        for hier_type_num, hier_type in hier_types.items():
            hier_sims_info = hier_types_top_sims[hier_type_num]
            taxonomy_skills_ix = hier_sims_info["taxonomy_skills_ix"]
            tax_info = taxonomy_skills.iloc[
                taxonomy_skills_ix[hier_sims_info["top_sim_indxs"][i][0]]
            ]
            match_results["top_" + hier_type + "_tax_level"] = (
                tax_info[skill_name_col],
                tax_info[skill_id_col],
                hier_sims_info["top_sim_scores"][i][0],
            )
        skill_mapper_list.append(match_results)

    # -----------------------------------------------------------------------
    # THE FOLLOWING IS NEW
    # -----------------------------------------------------------------------

    hier_name_mapper = load_s3_data(
        "open-jobs-lake",
        "escoe_extension/outputs/data/skill_ner_mapping/esco_hier_mapper.json",
    )

    skill_matches = final_prediction(
        skill_mapper_list,
        hier_name_mapper,
        match_thresholds_dict,
        num_hier_levels,
    )

    # Hard coded skill matches
    hard_coded_skills = load_s3_data(
        "open-jobs-lake",
        "escoe_extension/outputs/data/skill_ner_mapping/hardcoded_ojo_esco_lookup.json",
    )
    hard_coded_skills_dict = {}
    for hard_coded_skill in hard_coded_skills.values():
        hard_coded_skills_dict[hard_coded_skill["ojo_skill"]] = (
            hard_coded_skill["match_skill"],
            hard_coded_skill["match_id"],
            1,
        )

    # Create dict in form {skill: (esco_skill_name, esco_skill_id, sim_score)}
    all_extracted_skills_dict = {}
    for skill_match_info in skill_matches:
        skill = skill_match_info["ojo_skill"]
        if skill in hard_coded_skills_dict:
            all_extracted_skills_dict[skill] = hard_coded_skills_dict[skill]
        else:
            all_extracted_skills_dict[skill] = (
                skill_match_info["match_skill"],
                skill_match_info["match_id"],
                round(skill_match_info["match_score"], 3),
                skill_match_info["match_type"],
            )

    return all_extracted_skills_dict


# if __name__ == '__main__':
#     skill_ents = ['attention to detail', 'communication skills', 'Excel', 'communication', 'organisational skills', 'Word', 'somethingweirdthatwontbematched']

#     from dap_prinz_green_jobs.utils.bert_vectorizer import get_embeddings

#     all_extracted_skills_embeddings_dict = get_embeddings(skill_ents)

#     all_extracted_skills_dict = map_esco_skills(skill_ents, all_extracted_skills_embeddings_dict)

#     # 'attention to detail': ('attend to detail', '83e6510b-ffeb-4aec-959c-4265fd0ff7b7', 0.761, 'skill'),
#     # 'communication skills': ('communication', '15d76317-c71a-4fa2-aadc-2ecc34e627b7', 1)
