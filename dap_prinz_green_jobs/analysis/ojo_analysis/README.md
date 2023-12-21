## OJO Analysis

This folder contains scripts to aggregate data at the SIC-, SOC- and region-level.

To aggregate OJO data with extracted green measures (as defined in `ojo_analysis.yaml`), run the following commands:

```
python dap_prinz_green_jobs/analysis/ojo_analysis/aggregate_by_region.py #to aggregate by ITL regions
python dap_prinz_green_jobs/analysis/ojo_analysis/aggregate_by_soc.py #to aggregate by SOC codes
python dap_prinz_green_jobs/analysis/ojo_analysis/aggregate_by_sic.py #to aggregate by SIC codes
```

Meanwhile, the `process_ojo_green_measures.py` file contains methods for analysis. These are largely used in the `notebooks/` directory to generate graphs for the Green Jobs Explorer tool.

### Find similar occupations based of skills asked for

To use the outputs of `/aggregate_by_soc.py` and the skills found across all jobs adverts to find which occupations are most similar to each other, run:

```
python dap_prinz_green_jobs/analysis/ojo_analysis/occupation_similarity.py
```

This will create a matrix of the proportions of all skills per occupation, it will then compare these using cosine similarity to find the closest occupations to one another based off which skills are asked for.

We can then join the outputs of this to the original `/aggregate_by_soc.py` output by running:

```
python dap_prinz_green_jobs/analysis/ojo_analysis/aggregate_by_soc_part_2.py

```

This will create a file very similar to the output of `/aggregate_by_soc.py` but with an additional column containing the list of similar occupations.
