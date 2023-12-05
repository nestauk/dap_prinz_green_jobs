# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     comment_magics: true
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Chloropleth plot
# Average greenness by region
#

# %%
from dap_prinz_green_jobs.analysis.ojo_analysis.process_ojo_green_measures import *
from dap_prinz_green_jobs.getters.ojo_getters import (
    get_mixed_ojo_location_sample,
    get_mixed_ojo_salaries_sample,
)
from dap_prinz_green_jobs.getters.industry_getters import load_sic
from dap_prinz_green_jobs import BUCKET_NAME, logger, PROJECT_DIR
from dap_prinz_green_jobs.getters.data_getters import load_s3_data, save_to_s3
from dap_prinz_green_jobs.utils.chloropleth_utils import (
    get_nuts2polygons_dict,
    get_nuts1polygons_dict,
    get_nuts3polygons_dict,
)

import altair as alt
import geopandas as gpd

from datetime import datetime

import os

# %%
# save graphs
today = datetime.today().strftime("%y%m%d")
graph_dir = str(PROJECT_DIR / f"outputs/figures/green_jobs_explorer/{today}/")

if not os.path.exists(graph_dir):
    print(f"Creating {graph_dir} directory")
    os.makedirs(graph_dir)
else:
    print(f"{graph_dir} directory already exists")

# %% [markdown]
# ## Load the aggregated by region dataset

# %%
agg_itl_by = "itl_1_code"  # cant do 3 since altair cant deal with this much data
date_stamp = "20231205"
itl_aggregated_data = load_s3_data(
    BUCKET_NAME,
    f"outputs/data/ojo_application/extracted_green_measures/analysis/prop_green_skills_per_occ_{agg_itl_by}_{date_stamp}.csv",
)

# %%
df1 = pd.DataFrame(itl_aggregated_data[agg_itl_by].unique(), columns=[agg_itl_by])
df2 = pd.DataFrame(
    itl_aggregated_data["SOC_2020_name"].unique(), columns=["SOC_2020_name"]
)

blank_df = df1.merge(df2, how="cross")

itl_aggregated_data = itl_aggregated_data.merge(blank_df, how="outer")

# %% [markdown]
# ## Get additional geometry data needed for chloropleth

# %%
nuts1polygons_dict = get_nuts1polygons_dict()
itl1polygons_dict = {k.replace("UK", "TL"): v for k, v in nuts1polygons_dict.items()}

nuts2polygons_dict = get_nuts2polygons_dict()
itl2polygons_dict = {k.replace("UK", "TL"): v for k, v in nuts2polygons_dict.items()}

nuts3polygons_dict = get_nuts3polygons_dict()
itl3polygons_dict = {k.replace("UK", "TL"): v for k, v in nuts3polygons_dict.items()}

# %%
if agg_itl_by == "itl_1_code":
    itl_aggregated_data["geometry"] = itl_aggregated_data[agg_itl_by].apply(
        lambda x: itl1polygons_dict.get(x)[0]
    )
    itl_aggregated_data["itl_name"] = itl_aggregated_data[agg_itl_by].apply(
        lambda x: itl1polygons_dict.get(x)[1]
    )
    region_name = "ITL 1"
elif agg_itl_by == "itl_2_code":
    itl_aggregated_data["geometry"] = itl_aggregated_data[agg_itl_by].apply(
        lambda x: itl2polygons_dict.get(x, itl1polygons_dict.get(x))[0]
    )
    itl_aggregated_data["itl_name"] = itl_aggregated_data[agg_itl_by].apply(
        lambda x: itl2polygons_dict.get(x, itl1polygons_dict.get(x))[1]
    )
    region_name = "ITL 2"
else:
    itl_aggregated_data["geometry"] = itl_aggregated_data[agg_itl_by].apply(
        lambda x: itl3polygons_dict.get(x, itl1polygons_dict.get(x))[0]
    )
    itl_aggregated_data["itl_name"] = itl_aggregated_data[agg_itl_by].apply(
        lambda x: itl3polygons_dict.get(x, itl1polygons_dict.get(x))[1]
    )
    region_name = "ITL 3"

geo_df = gpd.GeoDataFrame(itl_aggregated_data)

# %% [markdown]
# ## Plot

# %%
select_box = alt.binding_select(
    options=list(geo_df["SOC_2020_name"].unique()), name="SOC 2020 EXT "
)
selection = alt.selection_point(
    value="Actuaries, economists and statisticians",
    fields=["SOC_2020_name"],
    bind=select_box,
)

regional_measures_plot = (
    alt.Chart(geo_df, title="Regional proportion of green skills")
    .mark_geoshape(invalid=None)
    .encode(
        color=alt.condition(
            "isValid(datum.mean_PROP_GREEN)",
            alt.Color(
                "mean_PROP_GREEN:O",
                title="",
                scale=alt.Scale(scheme="goldgreen", reverse=False),
                legend=None,
            ),
            alt.value("gray"),
        ),
        tooltip=[
            alt.Tooltip("SOC_2020_name", title="SOC_2020_name"),
            alt.Tooltip("itl_name", title=f"Region ({region_name})"),
            alt.Tooltip(
                "mean_PROP_GREEN",
                title="Average proportion of green skills per job advert",
                format=".3f",
            ),
            alt.Tooltip("num_job_ads", title="Number of job adverts"),
        ],
    )
    .add_selection(selection)
    .transform_filter(selection)
)

# %%
regional_measures_plot.save(
    f"{graph_dir}/green_measures_per_occ_{agg_itl_by}_chloropleth.html"
)

# %%
