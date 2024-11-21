# Updating measures when underlying job advert data updates

There are a few steps to follow when updating the green measures with a new batch of job adverts.

These will be described in this readme, and links shared for more information in other parts of the repo.

## Deduplicating the data

First of all, the new job advert data needs to be deduplicated. This is done by running:

```
python dap_prinz_green_jobs/pipeline/ojo_application/ojo_sample/data_refresh.py

```

Please read this [README](https://github.com/nestauk/dap_prinz_green_jobs/tree/dev/dap_prinz_green_jobs/pipeline/ojo_application/ojo_sample/README.md) under the 'Refreshing the data' section for more information.

## Run the green measures

Since it takes a long time to run, each green measure should be run separately. This will be done on only the new job adverts, and then afterwards the measures for the old adverts will be merged with the newly calculated ones.

Please read this [README](https://github.com/nestauk/dap_prinz_green_jobs/tree/dev/dap_prinz_green_jobs/pipeline/ojo_application/flows/README.md) under the 'Updating the data' section for more information.

Note: During the rerunning of the skills extraction in Nov 2024, the step where the skills extracted are mapped to the full ESCO taxonomy was removed. This was because of time constraints since it took a long time, and that the step was in some ways a duplication of effort. The OJO skills extracted are different to those calculated via the green skills measures. But we should be consistent, so from now on we can use the green skills extracted from the green measures, and then use the OJO skills data to provide any information about general skills. This will be across all the job adverts so there might be slight differences from our previous analysis.

## Process the skills data before aggregation

To speed up the aggregating step we process the skills datasets into more manageable forms. This is done by running

```
python dap_prinz_green_jobs/analysis/ojo_analysis/process_full_skills_data.py

```

And going forward information about the skills and green skills proportions are stored in 3 locations:

1. The skills extracted and mapped to ESCO (not just green) `s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/latest_update_20241114_skills.parquet` (with columns ['id', 'skill_label', 'esco_label', 'esco_id'])
2. The green skills extracted and mapped to green ESCO `outputs/data/ojo_application/extracted_green_measures/20241118/ojo_all_skills_green_measures_exploded_green.parquet` (with columns ['job_id', 'skill_label', 'extracted_green_skill', 'extracted_green_skill_id', 'green_skill_preferred_name'])
3. Information on the number of skills and proportion of green skills `outputs/data/ojo_application/extracted_green_measures/20241118/ojo_all_skills_green_measures_skill_metrics.parquet` (with columns ['job_id', 'prop_green_with_hs', 'NUM_ORIG_ENTS', 'NUM_SPLIT_ENTS', 'num_all_skills_ojo', 'count_green_skills_no_hs', 'PROP_GREEN'])

## Aggregate the data

Run

```
python dap_prinz_green_jobs/analysis/ojo_analysis/create_aggregated_data.py

```

to aggregate the data by SOC, SIC and ITL regions.

More about this can be found in this [README](https://github.com/nestauk/dap_prinz_green_jobs/tree/dev/dap_prinz_green_jobs/analysis/ojo_analysis/README.md).
