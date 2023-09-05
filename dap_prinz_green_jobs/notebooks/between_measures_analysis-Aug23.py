#!/usr/bin/env python
# coding: utf-8

# This notebook contains initial analysis _between_ measures of skills-, industries- and occupations on a sample of 100k job adverts
#
# This has been modified for the new format of the output from running greenmeasures

# In[1]:


from dap_prinz_green_jobs.getters.ojo_getters import (
    get_extracted_green_measures,
)

from dap_prinz_green_jobs.getters.occupation_getters import load_job_title_soc
from dap_prinz_green_jobs import BUCKET_NAME, logger, PROJECT_DIR
from dap_prinz_green_jobs.utils.bert_vectorizer import BertVectorizer
import pandas as pd
import numpy as np

import altair as alt

import umap
from sklearn.cluster import KMeans
import random
import os


# In[2]:


# save graphs

graph_dir = str(PROJECT_DIR / "outputs/figures/between_measure_analysis/160823/")

# make dir if it doesn't exist
if not os.path.exists(graph_dir):
    print(f"creating {graph_dir}")
    os.makedirs(graph_dir)
else:
    print(f"{graph_dir} already exists.")


# ### 0. Load relevant data for analysis
# Load extracted green measures at the skill-, occupation- and industry-level. Also load job titles to contextualise results.

# In[ ]:


from dap_prinz_green_jobs.getters.data_getters import load_s3_data
from dap_prinz_green_jobs import BUCKET_NAME


# In[ ]:


date_stamp = "20230816"
production = "True"
config = "base"

green_skills_outputs = load_s3_data(
    BUCKET_NAME,
    f"outputs/data/ojo_application/extracted_green_measures/{date_stamp}/ojo_sample_skills_green_measures_production_{production}_{config}.json",
)

green_occs_outputs = load_s3_data(
    BUCKET_NAME,
    f"outputs/data/ojo_application/extracted_green_measures/{date_stamp}/ojo_sample_occupation_green_measures_production_{production}_{config}.json",
)

green_inds_outputs = load_s3_data(
    BUCKET_NAME,
    f"outputs/data/ojo_application/extracted_green_measures/{date_stamp}/ojo_sample_industry_green_measures_production_{production}_{config}.json",
)


# In[ ]:


skill_measures_df = (
    pd.DataFrame.from_dict(green_skills_outputs, orient="index")
    .reset_index()
    .rename(columns={"index": "id"})
)
occs_measures_df = (
    pd.DataFrame.from_dict(green_occs_outputs, orient="index")
    .reset_index()
    .rename(columns={"index": "id"})
)
inds_measures_df = (
    pd.DataFrame.from_dict(green_inds_outputs, orient="index")
    .reset_index()
    .rename(columns={"index": "id"})
)


# In[ ]:


soc_occ_dict = (
    load_job_title_soc()
    .set_index("SOC 2020")["SOC 2020 UNIT GROUP DESCRIPTIONS"]
    .to_dict()
)


# ### 1. Merge and clean data so green measures are in a df
# Clean up green measures and produce two dataframes:
# 1. numerical green measures;
# 2. extracted green skills

# In[ ]:


print(len(skill_measures_df))
print(skill_measures_df["id"].nunique())
print(len(occs_measures_df))
print(occs_measures_df["id"].nunique())
print(len(inds_measures_df))
print(inds_measures_df["id"].nunique())
all_green_measures_df = pd.merge(
    skill_measures_df, occs_measures_df, how="outer", on="id"
)
all_green_measures_df = pd.merge(
    all_green_measures_df, inds_measures_df, how="outer", on="id"
)
all_green_measures_df["NUM_GREEN_ENTS"] = all_green_measures_df["GREEN_ENTS"].apply(len)
# Separate out the SOC columns
for soc_columns in ["SOC_2020_EXT", "SOC_2020", "SOC_2010", "name"]:
    all_green_measures_df[soc_columns] = all_green_measures_df["SOC"].apply(
        lambda x: x[soc_columns] if x else None
    )
all_green_measures_df.drop(columns=["SOC"], inplace=True)

all_green_measures_df.rename(
    columns={"name": "SOC_names", "id": "job_id"}, inplace=True
)
all_green_measures_df["SOC_2020_name"] = all_green_measures_df["SOC_2020"].map(
    soc_occ_dict
)

print(len(all_green_measures_df))
print(all_green_measures_df["job_id"].nunique())
print(all_green_measures_df.columns)
all_green_measures_df.head(2)


# In[ ]:


# get occupations for which we have over 50 job adverts for
representative_occs = (
    all_green_measures_df.groupby("SOC_2020_name")
    .job_id.count()
    .sort_values(ascending=False)
    .where(lambda x: x >= 50)
    .dropna()
    .keys()
    .tolist()
)

print(len(all_green_measures_df))
all_green_measures_df = all_green_measures_df[
    all_green_measures_df["SOC_2020_name"].isin(representative_occs)
].reset_index(drop=True)
print(len(all_green_measures_df))


# In[ ]:


all_green_measures_df["ENTS_GREEN_ENTS"] = all_green_measures_df.apply(
    lambda x: x["ENTS"] + x["GREEN_ENTS"], axis=1
)

green_skills_df = (
    all_green_measures_df[["job_id", "SOC_2020_name", "ENTS_GREEN_ENTS"]]
    .explode("ENTS_GREEN_ENTS")
    .reset_index(drop=True)
)
green_skills_df["skill_label"] = green_skills_df["ENTS_GREEN_ENTS"].apply(
    lambda x: x[0] if isinstance(x, list) else x
)
green_skills_df["extracted_skill"] = green_skills_df["ENTS_GREEN_ENTS"].apply(
    lambda x: x[1][0] if isinstance(x, list) else None
)
green_skills_df["extracted_skill_id"] = green_skills_df["ENTS_GREEN_ENTS"].apply(
    lambda x: x[1][1] if isinstance(x, list) else None
)
green_skills_df = green_skills_df[green_skills_df["skill_label"] != ""]


# Remove the duplicate green skills per job advert
print(len(green_skills_df))
green_skills_df.sort_values(by="extracted_skill", inplace=True)
green_skills_df.drop_duplicates(
    subset=["job_id", "skill_label"], keep="first", inplace=True
)
print(len(green_skills_df))

green_skills_df.head(2)


# 2. Analyse green measures between occupations, industries and skills
#
# Look at:
# - job adverts that have both **high** occupational greenness and **high** % of green skills
# - job adverts with **low** occupation greenness and **high** % of green skills
# - job adverts with **low** industry greeness (defined by high ghg emissions) and **high** occupation greenness
#
# Plot:
# - relationships between occupational, skill and industry greenness

# In[ ]:


# high occupation greenness (based on green share) and % of green skills
mean_green_timeshare_per_occ = (
    all_green_measures_df.groupby("SOC_2020_name")["GREEN TIMESHARE"]
    .agg(["mean"])
    .reset_index()
    .rename(columns={"mean": "mean_occupation_green_timeshare"})
)

green_skill_occ = (
    all_green_measures_df.groupby("SOC_2020_name")["PROP_GREEN"]
    .agg(["mean"])
    .reset_index()
    .rename(columns={"mean": "total_green_skills"})
    .merge(mean_green_timeshare_per_occ, on="SOC_2020_name", how="left")
)

# is there a correlation between the # of green skills requested and the greenness of an occupation?
print(
    "Correlation between the #mean  of green skills requested per occ and the greenness of an occupation: ",
    green_skill_occ["total_green_skills"].corr(
        green_skill_occ["mean_occupation_green_timeshare"]
    ),
)


# In[ ]:


# low occupation greenness and high % of green skills

non_green_occ_green_skills = (
    all_green_measures_df[all_green_measures_df["GREEN/NOT GREEN"] == "Non-green"]
    .groupby("SOC_2020_name")["PROP_GREEN"]
    .agg(["sum"])
    .reset_index()
    .rename(columns={"sum": "total_green_skills"})
    .sort_values(by="total_green_skills", ascending=False)
    .query("total_green_skills > 0")
)


# In[ ]:


non_green_occ_green_skills_list = non_green_occ_green_skills["SOC_2020_name"].to_list()

(
    green_skills_df.query("SOC_2020_name in @non_green_occ_green_skills_list")
    .groupby(["SOC_2020_name", "extracted_skill"])
    .job_id.count()
    .reset_index()
    .rename(columns={"job_id": "count"})
    .query("count > 10")
)

# looks like the green skills associated to non-green occupations relate primarily to health and safety regulations


# In[ ]:


# low industry greeness aka high ghg emissions and high occupation greenness

low_ind_high_occ_green = (
    all_green_measures_df[all_green_measures_df["GREEN/NOT GREEN"] == "Green"]
    .groupby("SOC_2020_name")["INDUSTRY TOTAL GHG EMISSIONS"]
    .agg(["mean"])
    .reset_index()
    .rename(columns={"mean": "mean_industry_ghg_emissions"})
    .sort_values(by="mean_industry_ghg_emissions", ascending=False)
)[:10]

print(
    f'green occupations with high industry ghg emissions include: {low_ind_high_occ_green["SOC_2020_name"].to_list()}'
)


# In[ ]:


# generate a dataframe with summed green measures per occupation

all_green_measures_df_occ = (
    all_green_measures_df.groupby("SOC_2020_name")
    .aggregate(
        {
            "INDUSTRY TOTAL GHG EMISSIONS": ["mean"],
            "GREEN TIMESHARE": ["mean"],
            "NUM_GREEN_ENTS": ["mean"],
            "PROP_GREEN": ["sum"],
        }
    )
    .reset_index()
)
all_green_measures_df_occ.columns = all_green_measures_df_occ.columns.levels[0]
all_green_measures_df_occ.columns = [
    "SOC_2020_name",
    "industry_ghg_emissions_mean",
    "occupation_green_timeshare_mean",
    "green_skills_count_mean",
    "green_skill_percentage_sum",
]

# pick majority occupation greenness
occ_green_cat = all_green_measures_df.groupby("SOC_2020_name")["GREEN CATEGORY"].agg(
    lambda x: pd.Series.mode(x)[0] if not all(pd.isnull(x)) else "None given"
)
# pick majority green/non-green occupation
occ_green_nongreen = all_green_measures_df.groupby("SOC_2020_name")[
    "GREEN/NOT GREEN"
].agg(lambda x: pd.Series.mode(x)[0] if not all(pd.isnull(x)) else "None given")
all_green_measures_df_occ["occ_green_non_green"] = all_green_measures_df_occ[
    "SOC_2020_name"
].map(occ_green_nongreen)
all_green_measures_df_occ["occ_green_category"] = all_green_measures_df_occ[
    "SOC_2020_name"
].map(occ_green_cat)


# In[ ]:


# industry greenness vs. occupational greenness

ind_occ_greeness = (
    alt.Chart(all_green_measures_df_occ)
    .mark_circle(size=60)
    .encode(
        x=alt.X("occupation_green_timeshare_mean", scale=alt.Scale(zero=False)),
        y=alt.Y("industry_ghg_emissions_mean", scale=alt.Scale(zero=False)),
        color=alt.Color(
            "occ_green_category",
            scale=alt.Scale(
                domain=[
                    "Non-Green",
                    "Green Enhanced Skills",
                    "Green New & Emerging",
                    "Green Increased Demand",
                    "None given",
                ],
                range=["red", "#013220", "green", "#90ee90", "black"],
            ),
        ),
        tooltip=[
            "SOC_2020_name",
            "green_skills_count_mean",
            "green_skill_percentage_sum",
        ],
    )
    .interactive()
)

# save graph
ind_occ_greeness.save(f"{graph_dir}/ind_occ_greeness.html")


# In[ ]:


# industry greenness vs. mean # of green skills requested

ind_skills_greeness = (
    alt.Chart(all_green_measures_df_occ)
    .mark_circle(size=60)
    .encode(
        x=alt.X("green_skill_percentage_sum", scale=alt.Scale(zero=False)),
        y=alt.Y("industry_ghg_emissions_mean", scale=alt.Scale(zero=False)),
        color=alt.Color(
            "occ_green_non_green",
            scale=alt.Scale(
                domain=["Non-green", "Green", "None given"],
                range=["red", "green", "black"],
            ),
        ),
        tooltip=[
            "SOC_2020_name",
            "green_skills_count_mean",
            "green_skill_percentage_sum",
        ],
    )
    .interactive()
)

ind_skills_greeness.save(f"{graph_dir}/ind_skills_greeness.html")


# In[ ]:


print("greening occupations:")
(
    all_green_measures_df_occ.dropna(
        subset=["industry_ghg_emissions_mean", "green_skill_percentage_sum"]
    ).sort_values(
        ["industry_ghg_emissions_mean", "green_skill_percentage_sum"],
        ascending=[True, False],
    )[
        :10
    ][
        "SOC_2020_name"
    ]
)


# In[ ]:


print("green occupations based on industry + green skills:")
(
    all_green_measures_df_occ.dropna(
        subset=["industry_ghg_emissions_mean", "green_skill_percentage_sum"]
    ).sort_values(
        ["green_skill_percentage_sum", "industry_ghg_emissions_mean"],
        ascending=[False, True],
    )[
        :10
    ][
        "SOC_2020_name"
    ]
)


# In[ ]:


print("brown occupations based on industry + green skills:")
(
    all_green_measures_df_occ.dropna(
        subset=["industry_ghg_emissions_mean", "green_skill_percentage_sum"]
    ).sort_values(
        ["industry_ghg_emissions_mean", "green_skill_percentage_sum"],
        ascending=[False, False],
    )[
        :10
    ][
        "SOC_2020_name"
    ]
)


# In[ ]:


# occupational greenness vs. # of green skills requested

occ_skill_greeness = (
    alt.Chart(all_green_measures_df_occ)
    .mark_circle(size=60)
    .encode(
        x=alt.X("green_skill_percentage_sum", scale=alt.Scale(zero=False)),
        y=alt.Y("occupation_green_timeshare_mean", scale=alt.Scale(zero=False)),
        color=alt.Color(
            "occ_green_non_green",
            scale=alt.Scale(domain=["Non-green", "Green"], range=["red", "green"]),
        ),
        tooltip=["SOC_2020_name", "green_skills_count_mean"],
    )
    .interactive()
)

occ_skill_greeness.save(f"{graph_dir}/occ_skill_greeness.html")

# list of "new" green skills

# list of "new" green occupations


# In[ ]:


# new green occupations (high % of green skills, low occ greeness)
print(
    "Occuptations with high % of green skills and low occupation greenness (based on green timeshare):"
)
(
    all_green_measures_df_occ.dropna(
        subset=["green_skill_percentage_sum", "occupation_green_timeshare_mean"]
    )
    # sort values by high green skills percentage and low occupation greenness
    .sort_values(
        ["green_skill_percentage_sum", "occupation_green_timeshare_mean"],
        ascending=[False, True],
    )[:10]
)["SOC_2020_name"].to_list()


# In[ ]:


# new green occupations (high % of green skills, low occ greeness)
print(
    "occupations with high % of green skills and low occupation greenness (based on green timeshare):"
)
high_green_skills_low_occ_list = (
    all_green_measures_df_occ.dropna(
        subset=["green_skill_percentage_sum", "occupation_green_timeshare_mean"]
    )
    # sort values by high green skills percentage and low occupation greenness
    .sort_values(
        ["green_skill_percentage_sum", "occupation_green_timeshare_mean"],
        ascending=[True, False],
    )[:10]
)["SOC_2020_name"].to_list()
print(high_green_skills_low_occ_list)


# ## NEW GREEN SKILLS

# In[ ]:


new_green_skills = list(
    set(
        green_skills_df[
            (green_skills_df["SOC_2020_name"].isin(high_green_skills_low_occ_list))
            & (green_skills_df.extracted_skill.isna())
        ].skill_label
    )
)

bert_model = BertVectorizer().fit()
new_green_skills_embeds = bert_model.transform(new_green_skills)


# In[ ]:


# reduce embeds
reducer = umap.UMAP()
embedding = reducer.fit_transform(new_green_skills_embeds)
kmeans = KMeans(n_clusters=30, random_state=0).fit(embedding)
labels = kmeans.labels_


# In[ ]:


new_skill_cluster_df = pd.DataFrame(
    {
        "skill": new_green_skills,
        "cluster_number": labels,
        "x": embedding[:, 0],
        "y": embedding[:, 1],
    }
)


# In[ ]:


alt.data_transformers.disable_max_rows()

new_green_skills = (
    alt.Chart(
        new_skill_cluster_df,
        title='new "green" skill groups based on high green occupation greenness and low % of green skills requested',
    )
    .mark_circle(size=60)
    .encode(
        x="x",
        y="y",
        color=alt.Color("cluster_number", legend=None),
        tooltip=["skill", "cluster_number"],
    )
    .interactive()
)

new_green_skills

new_green_skills.save(f"{graph_dir}/new_green_skills.html")


# In[ ]:


green_occs = list(
    all_green_measures_df_occ[
        all_green_measures_df_occ["occ_green_non_green"] == "Green"
    ]["SOC_2020_name"]
)


# In[ ]:


# #reduce embeds
random.seed(50)
green_occ = random.choice(green_occs)

new_green_skills = list(
    set(
        green_skills_df[
            (green_skills_df["SOC_2020_name"] == green_occ)
            & (green_skills_df.extracted_skill.isna())
        ].skill_label
    )
)
new_green_skills_embeds = bert_model.transform(new_green_skills)
embedding = reducer.fit_transform(new_green_skills_embeds)
kmeans = KMeans(n_clusters=15, random_state=0).fit(embedding)
labels = kmeans.labels_

new_skill_cluster_df = pd.DataFrame(
    {
        "skill": new_green_skills,
        "cluster_number": labels,
        "x": embedding[:, 0],
        "y": embedding[:, 1],
    }
)

alt.data_transformers.disable_max_rows()

occ1_new_skills = (
    alt.Chart(
        new_skill_cluster_df,
        title=f'new "green" skill groups based on green "{green_occ}"occupation',
    )
    .mark_circle(size=60)
    .encode(
        x="x",
        y="y",
        color=alt.Color("cluster_number", legend=None),
        tooltip=["skill", "cluster_number"],
    )
    .interactive()
)

occ1_new_skills.save(f"{graph_dir}/occ1_new_skills.html")


# In[ ]:


# reduce embeds
random.seed(57)
green_occ = random.choice(green_occs)

new_green_skills = list(
    set(
        green_skills_df[
            (green_skills_df["SOC_2020_name"] == green_occ)
            & (green_skills_df.extracted_skill.isna())
        ].skill_label
    )
)
new_green_skills_embeds = bert_model.transform(new_green_skills)
embedding = reducer.fit_transform(new_green_skills_embeds)
kmeans = KMeans(n_clusters=10, random_state=0).fit(embedding)
labels = kmeans.labels_

new_skill_cluster_df = pd.DataFrame(
    {
        "skill": new_green_skills,
        "cluster_number": labels,
        "x": embedding[:, 0],
        "y": embedding[:, 1],
    }
)

alt.data_transformers.disable_max_rows()

occ2_new_skills = (
    alt.Chart(
        new_skill_cluster_df,
        title=f'new "green" skill groups based on green "{green_occ}"occupation',
    )
    .mark_circle(size=60)
    .encode(
        x="x",
        y="y",
        color=alt.Color("cluster_number", legend=None),
        tooltip=["skill", "cluster_number"],
    )
    .interactive()
)

occ2_new_skills.save(f"{graph_dir}/occ2_new_skills.html")


# In[ ]:


# reduce embeds
random.seed(12)
green_occ = random.choice(green_occs)

new_green_skills = list(
    set(
        green_skills_df[
            (green_skills_df["SOC_2020_name"] == green_occ)
            & (green_skills_df.extracted_skill.isna())
        ].skill_label
    )
)
new_green_skills_embeds = bert_model.transform(new_green_skills)
embedding = reducer.fit_transform(new_green_skills_embeds)
kmeans = KMeans(n_clusters=10, random_state=0).fit(embedding)
labels = kmeans.labels_

new_skill_cluster_df = pd.DataFrame(
    {
        "skill": new_green_skills,
        "cluster_number": labels,
        "x": embedding[:, 0],
        "y": embedding[:, 1],
    }
)

alt.data_transformers.disable_max_rows()

occ3_new_skills = (
    alt.Chart(
        new_skill_cluster_df,
        title=f'new "green" skill groups based on green "{green_occ}"occupation',
    )
    .mark_circle(size=60)
    .encode(
        x="x",
        y="y",
        color=alt.Color("cluster_number", legend=None),
        tooltip=["skill", "cluster_number"],
    )
    .interactive()
)

occ3_new_skills.save(f"{graph_dir}/occ3_new_skills.html")


# ### Next steps
#
# 1. **Skills improvement**: looks like the green skill 'health and safety regulation' heavily skews skills-based results. We will also need to develop a method to determine if unmatched skill clusters are indeed green, even if the occupation is considered green.
#
# 2. **Better sample**: given how few job adverts contain 'green' skills, should we engineer a sample that artificially would contain more green skills?
