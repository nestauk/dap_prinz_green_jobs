# OJO Sampling

This directory contains scripts to generate samples of OJO data.

## Deduplication

To first find which job adverts in the OJO database are duplicates run:

```
python dap_prinz_green_jobs/pipeline/ojo_application/ojo_sample/deduplication.py

```

this creates a csv with deduplicated job adverts ids in a certain time window. The default is a 7 day window; so this means if two adverts with the same text and location turn up within a week of each other, then only one will remain in the outputted data.

## Sampling

To sample the deduplicated dataset run:

```
python dap_prinz_green_jobs/pipeline/ojo_application/ojo_sample/sample_ojo.py

```

this will create a sample of the deduplicated OJO dataset and output all the OJO tables filtered by this sample. All outputs will go to the `s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/` S3 folder. The main file of interest is `s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/ojo_sample.csv` which contains the job advert descriptions, the location, date, and the job title.
