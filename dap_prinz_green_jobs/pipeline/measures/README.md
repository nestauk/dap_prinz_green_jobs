# Pipeline Measures

## Occupational Green Measures

Run

```
python dap_prinz_green_jobs/pipeline/measures/occupation_measures.py
```

to find which job adverts are for occupations which are green or not.

### Datasets

- `indexsocextv5updated.xlsx`: A dataset of the SOC codes for each job title can be found on the ONS website [here](https://www.ons.gov.uk/methodology/classificationsandstandards/standardoccupationalclassificationsoc/standardoccupationalclassificationsocextensionproject). A download of the "SOC 2020 6-digit index (2.7 MB xlsx)" downloaded as of 25/04/23 was uploaded to S3 (`s3://prinz-green-jobs/inputs/data/occupation_data/ons/indexsocextv5updated.xlsx`).
- `Summary of green occupations (Nov 2021).xlsx`: The Greater London Authority have mapped the [ONET green occupations codes](https://www.onetcenter.org/green/skills.html) to UK SOC. This data is available to download [here](https://data.london.gov.uk/dataset/identifying-green-occupations-in-london?_gl=1%2a8t5yr7%2a_ga%2aNzIwMzA5OTAwLjE2ODE5NzgzODk.%2a_ga_PY4SWZN1RJ%2aMTY4MjQzNTQxNS4xLjAuMTY4MjQzNTQyMC41NS4wLjA.) and a description about its creation is [here](https://www.london.gov.uk/business-and-economy-publications/identifying-green-occupations-london#useful-links). A download of the "Summary of green occupations (Nov 2021).xlsx" downloaded as of 25/04/23 was uploaded to S3 (`s3://prinz-green-jobs/inputs/data/occupation_data/gla/Summary of green occupations (Nov 2021).xlsx`).
