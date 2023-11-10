# OJO analysis

## Processed datasets

Create a dataset of green measures aggregated by occupation by running

```
python dap_prinz_green_jobs/analysis/ojo_analysis/aggregate_by_soc.py

```

This will create a csv `outputs/data/ojo_application/extracted_green_measures/analysis/occupation_aggregated_data_{DATE}.csv`, where each row is aggregated green measures information for each occupation.

Create a dataset of green measures aggregated by region (ITL3) by running

```
python dap_prinz_green_jobs/analysis/ojo_analysis/aggregate_by_region.py

```

This will create a csv `outputs/data/ojo_application/extracted_green_measures/analysis/itl_3_code_aggregated_data_{DATE}.csv`, where each row is aggregated green measures information for each occupation.
