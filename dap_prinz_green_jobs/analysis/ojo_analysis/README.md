## OJO Analysis

This folder contains scripts to aggregate data at the SIC-, SOC- and region-level.

### Skills formatting

To speed up the aggregating step we process the skills datasets into more manageable forms. This is done by running

```
python dap_prinz_green_jobs/analysis/ojo_analysis/process_full_skills_data.py

```

And going forward information about the skills and green skills proportions are stored in 3 locations:

1. The skills extracted and mapped to ESCO (not just green) `s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/latest_update_20241114_skills.parquet` (with columns ['id', 'skill_label', 'esco_label', 'esco_id']). A smaller version of this file with just the job advert and ESCO id columns is in `s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/20241118/ojo_all_skills_exploded.parquet`.
2. The green skills extracted and mapped to green ESCO `outputs/data/ojo_application/extracted_green_measures/20241118/ojo_all_skills_green_measures_exploded_green.parquet` (with columns ['job_id', 'skill_label', 'extracted_green_skill', 'extracted_green_skill_id', 'green_skill_preferred_name'])
3. Information on the number of skills and proportion of green skills `outputs/data/ojo_application/extracted_green_measures/20241118/ojo_all_skills_green_measures_skill_metrics.parquet` (with columns ['job_id', 'prop_green_with_hs', 'NUM_ORIG_ENTS', 'NUM_SPLIT_ENTS', 'num_all_skills_ojo', 'count_green_skills_no_hs', 'PROP_GREEN'])

### Data aggregation

To aggregate OJO data with extracted green measures (as defined in `ojo_analysis.yaml`), run:

```
python dap_prinz_green_jobs/analysis/ojo_analysis/create_aggregated_data.py

```

to aggregate the data by SOC, SIC and ITL regions. This script draws on functions from `process_ojo_green_measures.py`.

This will also format the occupation aggregated data into a form suitable for the Green Jobs Explorer website - these are very superficial changes needed to create the website - e.g. changing single to double quotation marks.

### Finding similar occupations based of skills asked for

In `create_aggregated_data.py` the similarities of occupations are also created using functions from `occupation_similarity.py`. To do this, a matrix of the proportions of all skills per occupation is created, and then each row of this matrix is compared using cosine similarity to find the closest occupations to one another based off which skills are asked for. The output `occupation_aggregated_data_{DATE}_extra.csv` contains an additional column containing the list of similar occupations.

### Final files

Lots of files are outputted in the `s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/analysis/20241121/` folder from the previous scripts, the most important for analysis are:

1. `occupation_aggregated_data_20241121_extra_gjeformat.csv`: The data which powers the Green Jobs Explorer website. This is the aggregated data per occupation (SOC_EXT) with occupations with less than 50 job adverts removed.
2. `industry_aggregated_data_20241121.csv`: The data aggregated by SIC.
3. `all_itl_aggregated_data_20241121.csv`: The data aggregated by each of ITL 1, 2 and 3.
