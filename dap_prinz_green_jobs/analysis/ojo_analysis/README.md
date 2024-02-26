## OJO Analysis

This folder contains scripts to aggregate data at the SIC-, SOC- and region-level.

### Skills formatting

To aggregate the data we need to utilise all the skills per job advert and create a new dataset which has a single skill per row.

This process takes a long time but is used by all the aggregation scripts, so we can first create it by running:

```
python dap_prinz_green_jobs/analysis/ojo_analysis/process_full_skills_data.py

```

This creates a file named `exploded_all_ojo_large_sample_skills_green_measures_production_True.csv` which will be stored in the same date stamped S3 folder as the extracted skills were.

### Data aggregation

To aggregate OJO data with extracted green measures (as defined in `ojo_analysis.yaml`), run the following commands:

```
python dap_prinz_green_jobs/analysis/ojo_analysis/aggregate_by_region.py #to aggregate by ITL regions
python dap_prinz_green_jobs/analysis/ojo_analysis/aggregate_by_soc.py #to aggregate by SOC codes
python dap_prinz_green_jobs/analysis/ojo_analysis/aggregate_by_sic.py #to aggregate by SIC codes
```

Meanwhile, the `process_ojo_green_measures.py` file contains methods for analysis. These are largely used in the `notebooks/` directory to generate graphs for the Green Jobs Explorer tool.

### Finding similar occupations based of skills asked for

In `aggregate_by_soc.py` the similarities of occupations are also created using functions from `occupation_similarity.py`. To do this, a matrix of the proportions of all skills per occupation is created, and then each row of this matrix is compared using cosine similarity to find the closest occupations to one another based off which skills are asked for. The output `occupation_aggregated_data_{DATE}_extra.csv` contains an additional column containing the list of similar occupations.
