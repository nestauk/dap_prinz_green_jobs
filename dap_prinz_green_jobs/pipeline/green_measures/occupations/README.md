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
- `greentimesharesoc.xlsx`: A dataset of the green time shares for each SOC code. The data is available to download [here](https://www.ons.gov.uk/economy/environmentalaccounts/datasets/estimatedtimespentongreentasksbyoccupationcode) with methodology [here](https://www.ons.gov.uk/economy/environmentalaccounts/articles/developingamethodformeasuringtimespentongreentasks/march2022). A download of it was downloaded on 23/05/23 and uploaded to S3 [here](s3://prinz-green-jobs/inputs/data/occupation_data/ons/greentimesharesoc.xlsx).
- `Occupations_for_all_green_topics.csv`: The O\*NET green topics per occupation dataset downloaded from [here](https://www.onetonline.org/search/green_topics/) on 07/07/23. The report describing this data can be found [here](https://www.onetcenter.org/reports/Green_Topics.html).

## Green measures

Measures based off the job adverts predicted 2010 SOC:

- GREEN CATEGORY : The green category ("Green Enhanced Skills", "Green Increased Demand", "Green New & Emerging", "Non-Green") from the GLA dataset - These were found via "a mapping exercise between US (O\*NET) and UK (SOC 2010) occupational taxonomies using a crosswalk derived from the LMI for All API. This exercise involves a degree of judgement and the list of green occupations remains subject to development." From `Summary of green occupations (Nov 2021).xlsx`.
- GREEN/NOT GREEN : Green or non-green categories from the GLA data, again based off O\*NET's 2011 work. From `Summary of green occupations (Nov 2021).xlsx`.
- GREEN TIMESHARE : Estimates of the fraction of time spent doing green tasks (using O\*NET 2011 green task definitions). From `greentimesharesoc.xlsx`.
- GREEN TOPICS : O\*NET's 2022 work on green topics linked to US occupations. From `Occupations_for_all_green_topics.csv`.

## Usage

```
from dap_prinz_green_jobs.pipeline.green_measures.occupations.occupations_measures_utils import OccupationMeasures

om = OccupationMeasures()
om.load()
unique_job_titles = ["Data Scientist", "Nurse", "Key Stage 4 teacher", "Pharmacist", "Biologist"]
job_title_2_match = om.precalculate_soc_mapper(unique_job_titles)

om.get_green_measure_for_job_title("Data Scientist")
>>> {'GREEN CATEGORY': 'Non-Green', 'GREEN/NOT GREEN': 'Non-green', 'GREEN TIMESHARE': 12.9, 'GREEN TOPICS': 21, 'SOC': {'SOC_2020_EXT': '2433/02', 'SOC_2020': '2433', 'SOC_2010': '2114'}}

or

om.get_measures(job_adverts= [{'description': 'We are looking for a sales ...', 'job_title': 'Data Scientist'}], job_title_key='job_title')
>>> [{'GREEN CATEGORY': 'Non-Green', 'GREEN/NOT GREEN': 'Non-green', 'GREEN TIMESHARE': 12.9, 'GREEN TOPICS': 21, 'SOC': {'SOC_2020_EXT': '2433/02', 'SOC_2020': '2433', 'SOC_2010': '2114'}}]

```
