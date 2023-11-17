# OJO Sampling

This directory contains scripts to generate samples of OJO data.

## Deduplication

To first find which job adverts in the OJO database are duplicates run:

```
python dap_prinz_green_jobs/pipeline/ojo_application/ojo_sample/deduplication.py
```

this creates a csv with deduplicated job adverts ids in a certain time window (if desired). The default is not to use a time window; so this means any instance (time independent) of a job adverts having the same text and location will be deduplicated. If a time window is used (e.g. 7 days) then if two adverts with the same text and location turn up within a week of each other, then only one will remain in the outputted data.

## Sampling

To generate a sample of 1,000,000 deduplicated job ads that are sampled per SOC4 code per ITL2 code, you first need to extract SOC codes across all job adverts. To do this run:

```
python dap_prinz_green_jobs/pipeline/ojo_application/ojo_sample/get_soc_codes.py
```

To generate deduplicated datasets for:

- A random small sample (100,000);
- An engineered 'green' sample based on keywords (100,000) and;
- The final sample of 1,000,000 job ads sampled by soc4 and itl2 code

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
