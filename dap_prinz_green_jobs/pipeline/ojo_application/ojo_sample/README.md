# OJO Sampling

This directory contains scripts to generate samples of OJO data.

## Deduplication

To first find which job adverts in the OJO database are duplicates run:

```
python dap_prinz_green_jobs/pipeline/ojo_application/ojo_sample/deduplication.py
```

this creates a csv with deduplicated job adverts ids in a certain time window (if desired). The default is not to use a time window; so this means any instance (time independent) of a job adverts having the same text and location will be deduplicated. If a time window is used (e.g. 7 days) then if two adverts with the same text and location turn up within a week of each other, then only one will remain in the outputted data.

## Sampling

To generate deduplicated datasets for:

- A random small sample (100,000);
- An engineered 'green' sample based on keywords (100,000) and;
- The final random sample of 1,000,000 job ads

run:

```
python dap_prinz_green_jobs/pipeline/ojo_application/ojo_sample/sample_ojo.py

```

This will create a sample of the deduplicated OJO datasets and output all the OJO tables filtered by this sample. All outputs will go to the `s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/` S3 folder.

There are multiple main files of interest:

- `s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/ojo_sample.csv` (100,000 job ads)
- `s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/large_ojo_sample.csv` (1,000,000 job ads)
- `s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/green_ojo_sample.csv` (~40,000 job ads with green keywords)

All files contain the job advert descriptions, the location, date, and the job title. The `large_ojo_sample.csv` file will be our final sample to run our models on for downstream analysis.

Since green jobs are rare in our dataset, the green sample was generated as a way to test out our approaches on jobs that are likely to be green. We recognise the keyword search created dataset is not a conclusive list, will pick up false positives, and will also miss many green jobs. This list or the way it's been generated will not be used to make any comment on greenness - just as a useful dataset for this projects development.

## Refreshing the data

When there is new OJO data added you can run the deduplication and filtering steps on the new batch of job adverts by first editing and then running:

```
python dap_prinz_green_jobs/pipeline/ojo_application/ojo_sample/data_refresh.py

```

The file paths at the top of this script will need editing to the latest data locations for the full OJO data.

The datasets will then be saved out to a datestamped folder, e.g. `s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/`. This will just contain the data for the newest job adverts.

| Date ran | Min created date | Max created data | Number of unique job adverts in deduplicated data | Job ids data location                                                                                      |
| -------- | ---------------- | ---------------- | ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| 20240213 | 11/12/2020       | 06/11/2023       | 4,653,782                                         | s3://prinz-green-jobs/outputs/ data/ojo_application/deduplicated_sample/ deduplicated_job_ids.csv          |
| 20241114 | 07/11/2023       | 05/11/2024       | 1,313,447                                         | s3://prinz-green-jobs/outputs/ data/ojo_application/deduplicated_sample/ 20241114/deduplicated_job_ids.csv |
| 20250627 | 06/11/2024       | 22/04/2025       | 178,288                                           | s3://prinz-green-jobs/outputs/ data/ojo_application/deduplicated_sample/ 20250627/deduplicated_job_ids.csv |

It will also contain all the data concatenated for the most key columns for the combines, deduplicated 5,967,229 job adverts from 11/12/2020 to 05/11/2024:

1. `s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/latest_update_20241114_descriptions.parquet` (3.4GB) - the job ids and the full advert text (['id', 'description'])
2. `s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/20241114/latest_update_20241114_key_columns.parquet` (55MB) - extra info about the advert, but not the full advert text (['id', 'job_title_raw', 'created', 'itl_3_code', 'itl_3_name'])
