## OJO Analysis

This folder contains scripts to aggregate data at the SIC-, SOC- and region-level.

To aggregate OJO data with extracted green measures (as defined in `ojo_analysis.yaml`), run the following commands:

```
python dap_prinz_green_jobs/analysis/ojo_analysis/aggregate_by_region.py #to aggregate by ITL regions
python dap_prinz_green_jobs/analysis/ojo_analysis/aggregate_by_soc.py #to aggregate by SOC codes
python dap_prinz_green_jobs/analysis/ojo_analysis/aggregate_by_sic.py #to aggregate by SIC codes
```

Meanwhile, the `process_ojo_green_measures.py` file contains methods for analysis. These are largely used in the `notebooks/` directory to generate graphs for the Green Jobs Explorer tool.
