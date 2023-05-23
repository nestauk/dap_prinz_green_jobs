# Occupational Green Measures

The script

```
dap_prinz_green_jobs/pipeline/green_measures/occupations/occupation_measures_utils.py
```

contains functions needed to see whether occupations are green or not.

The class in

```
dap_prinz_green_jobs/pipeline/green_measures/occupations/soc_map.py
```

can be used to map job title(s) to SOC codes, for example

```
from dap_prinz_green_jobs.pipeline.green_measures.occupations.soc_map import SOCMapper

soc_mapper = SOCMapper()
soc_mapper.load()
soc_mapper.get_soc(job_titles=["data scientist", "Assistant nurse", "Senior financial consultant - London"])
>>> [('2425', 'data scientist'), ('6141', 'assistant nurse'), ('3534', 'financial consultant')]
```

## Datasets used

- `indexsocextv5updated.xlsx`: A dataset of the SOC codes for each job title can be found on the ONS website [here](https://www.ons.gov.uk/methodology/classificationsandstandards/standardoccupationalclassificationsoc/standardoccupationalclassificationsocextensionproject). A download of the "SOC 2020 6-digit index (2.7 MB xlsx)" downloaded as of 25/04/23 was uploaded to S3 (`s3://prinz-green-jobs/inputs/data/occupation_data/ons/indexsocextv5updated.xlsx`).
- `Summary of green occupations (Nov 2021).xlsx`: The Greater London Authority have mapped the [ONET green occupations codes](https://www.onetcenter.org/green/skills.html) to UK SOC. This data is available to download [here](https://data.london.gov.uk/dataset/identifying-green-occupations-in-london?_gl=1%2a8t5yr7%2a_ga%2aNzIwMzA5OTAwLjE2ODE5NzgzODk.%2a_ga_PY4SWZN1RJ%2aMTY4MjQzNTQxNS4xLjAuMTY4MjQzNTQyMC41NS4wLjA.) and a description about its creation is [here](https://www.london.gov.uk/business-and-economy-publications/identifying-green-occupations-london#useful-links). A download of the "Summary of green occupations (Nov 2021).xlsx" downloaded as of 25/04/23 was uploaded to S3 (`s3://prinz-green-jobs/inputs/data/occupation_data/gla/Summary of green occupations (Nov 2021).xlsx`).
