# 🏭🥬 Industry Green Measures

This directory is organised as follows:

1. `sic_mapper/`: The directory that maps job adverts to SIC codes.
2. `prodigy/`: The directory to generate training data to train a company description classifier for the `SicMapper`.
3. `evaluation/`: The directory to evaluate the `SicMapper` class.

It also contains the following `.py` files:

1. `industries_data_processing.py`: A script to process the Companies House dataset.
2. `industres_measures_utils.py`: Utils associated to the `IndustryMeasures` class.
3. `industries_measures.py`: The main class to calculate green industry measures for a job advert.

## 🔨 `IndustryMeasures` core functionality

To map job adverts to SIC codes and get SIC-level industry measures, you can use the `IndustryMeasures` class in `industries_measures.py`:

```
from dap_prinz_green_jobs.pipeline.green_measures.industries.industries_measures import IndustryMeasures

job_ads = {'id': 1, 'job_text': 'We are looking for a software engineer to join our team. This company sits in the software engineering industry.'}

im = IndustryMeasures() #instantiate the class
im.load() #load the relevant green industries datasets and SicMapper class

im.get_measures(job_ads) #get the measures for the job advert

>>  {1: {'SIC': '62012',
  'SIC_name': 'Business and domestic software development',
  'SIC_confidence': 0.62,
  'SIC_method': 'closest distance',
  'company_description': 'This company sits in the software engineering industry..',
  'INDUSTRY TOTAL GHG EMISSIONS': 254,
  'INDUSTRY GHG PER UNIT EMISSIONS': 0,
  'INDUSTRY PROP HOURS GREEN TASKS': 9.700000000000001,
  'INDUSTRY PROP WORKERS GREEN TASKS': 43.5,
  'INDUSTRY PROP WORKERS 20PERC GREEN TASKS': 23.599999999999998,
  'INDUSTRY GHG EMISSIONS PER EMPLOYEE': 0.6,
  'INDUSTRY CARBON DIOXIDE EMISSIONS PER EMPLOYEE': 771.2}}
```

## 🪥 Industry data processing

As a one-off run:

```
python dap_prinz_green_jobs/pipeline/green_measures/industries/industries_data_processing.py
```

This will save out just the key columns from the full Companies House dataset to `s3://prinz-green-jobs/inputs/data/industry_data/BasicCompanyDataAsOneFile-2023-05-01_key_columns_only.csv`. This speeds up the rest of the pipeline as loading the full dataset is slow. NOTE: this script will take a long time to run.

## 💾 Datasets used & green measures

- `BasicCompanyDataAsOneFile-2023-05-01.csv`: A snapshot of the Companies House dataset was downloaded from the gov website [here](http://download.companieshouse.gov.uk/en_output.html). The "BasicCompanyDataAsOneFile-2023-05-01.zip (439Mb)" file was downloaded and unzipped as of 02/05/2023 and then uploaded to S3 (`s3://prinz-green-jobs/inputs/data/industry_data/BasicCompanyDataAsOneFile-2023-05-01.csv`).
- `BasicCompanyDataAsOneFile-2023-05-01_key_columns_only.csv`: Just the important columns from the above Companies House data (created by running `companies_house_processing.py`).
- `atmosphericemissionsghg.xlsx`: The total greenhouse gas emissions by SIC from the ONS website [here](https://www.ons.gov.uk/economy/environmentalaccounts/datasets/ukenvironmentalaccountsatmosphericemissionsgreenhousegasemissionsbyeconomicsectorandgasunitedkingdom). The current edition of the dataset was downloaded as of 02/05/23 and uploaded to S3 (`s3://prinz-green-jobs/inputs/data/industry_data/atmosphericemissionsghg.xlsx`).
- `atmosphericemissionsghgintensity.xlsx`: The greenhouse gas emissions per unit of economic output by SIC from the ONS website [here](https://www.ons.gov.uk/economy/environmentalaccounts/datasets/ukenvironmentalaccountsatmosphericemissionsgreenhousegasemissionsintensitybyeconomicsectorunitedkingdom/current). The most current version of the dataset was downloaded on 13/06/2023 and uploaded to S3 (`s3://prinz-green-jobs/inputs/data/industry_data/atmosphericemissionsghgintensity.xlsx`).
- `publisheduksicsummaryofstructureworksheet.xlsx`: The ONS SIC codes downloaded from [here](https://www.ons.gov.uk/methodology/classificationsandstandards/ukstandardindustrialclassificationofeconomicactivities/uksic2007). The 'UK SIC 2007 Summary of Structure Worksheet' xlsx was downloaded on 02/05/2023 and uploaded to S3 (`s3://prinz-green-jobs/inputs/data/industry_data/publisheduksicsummaryofstructureworksheet.xlsx`).
- `greentasks.xlsx`: The ONS time spent on green tasks from [here](https://www.ons.gov.uk/economy/environmentalaccounts/datasets/timespentongreentasks) - downloaded on 12/06/2023 and uploaded to S3 (`s3://prinz-green-jobs/inputs/data/industry_data/greentasks.xlsx`). "Experimental estimates of the time spent doing green tasks, over time, by UK country and by industry. The estimates use a new method based on task-level data from the ONET database in the US."
- `emissionsperemployee.xlsx`: The [ONS's emissions per employee by industry, United Kingdom, 2021 here](https://www.ons.gov.uk/economy/environmentalaccounts/datasets/emissionsperemployeeuk2015to2021). The most current version of the dataset was downloaded on 03/10/2023 and uploaded to S3 (s3://prinz-green-jobs/inputs/data/industry_data/emissionsperemployee.xlsx).

Measures based of the job adverts predicted SIC are as follows:

- **INDUSTRY TOTAL GHG EMISSIONS :** The 2020 total greenhouse gas emissions for this SIC section code (letter) or division code (two digit). From `atmosphericemissionsghg.xlsx`. -**INDUSTRY GHG PER UNIT EMISSIONS :** The 2021 greenhouse gas emissions per unit of economic activity for this SIC section code (letter) or division code (two digit). From `atmosphericemissionsghgintensity.xlsx`. -**INDUSTRY PROP HOURS GREEN TASKS :** The 2019 proportion of hours worked spent doing green tasks for this SIC section code (letter). From `greentasks.xlsx`. -**INDUSTRY PROP WORKERS GREEN TASKS :** The 2019 proportion of workers doing green tasks for this SIC section code (letter). From `greentasks.xlsx`. -**INDUSTRY PROP WORKERS 20PERC GREEN TASKS :** The 2019 proportion of workers spending at least 20% of their time doing green tasks for this SIC section code (letter). From `greentasks.xlsx`. -**INDUSTRY GHG EMISSIONS PER EMPLOYEE :** The 2021 greenhouse gas emissions per employee for this SIC section code (letter) or division code (two digit). From `emissionsperemployee.xlsx`. -**INDUSTRY CARBON DIOXIDE EMISSIONS PER EMPLOYEE :** The 2021 carbon dioxide emissions per employee for this SIC section code (letter) or division code (two digit). From `emissionsperemployee.xlsx`.
