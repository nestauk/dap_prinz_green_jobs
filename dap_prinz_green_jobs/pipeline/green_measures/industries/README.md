# Industry Green Measures

As a one-off run:

```
python dap_prinz_green_jobs/pipeline/green_measures/industries/industries_data_processing.py
```

This will save out just the key columns from the full Companies House dataset to `s3://prinz-green-jobs/inputs/data/industry_data/BasicCompanyDataAsOneFile-2023-05-01_key_columns_only.csv`. This speeds up the rest of the pipeline as loading the full dataset is slow. NOTE: this script will take a long time to run.

The script

```
dap_prinz_green_jobs/pipeline/green_measures/industries/industry_measures_utils.py
```

contains functions needed to see whether industries are green or not.

Finally, the generate a dataset of SIC codes described as company descriptions for downstream mapping, run as a one off:

```
export OPENAI_API_KEY="sk-xxx" #expore your openAI key in your terminal
python dap_prinz_green_jobs/pipeline/green_measures/industries/sic_data_processing.py
```

Please note that this script takes a long time to run in production. If you would like to run this on all SIC codes, you will need to pass the `--production` flag.

As we are using an LLM to generate company descriptions of SIC codes, results will vary every time you run the script.

## Datasets used

- `BasicCompanyDataAsOneFile-2023-05-01.csv`: A snapshot of the Companies House dataset was downloaded from the gov website [here](http://download.companieshouse.gov.uk/en_output.html). The "BasicCompanyDataAsOneFile-2023-05-01.zip (439Mb)" file was downloaded and unzipped as of 02/05/2023 and then uploaded to S3 (`s3://prinz-green-jobs/inputs/data/industry_data/BasicCompanyDataAsOneFile-2023-05-01.csv`).
- `BasicCompanyDataAsOneFile-2023-05-01_key_columns_only.csv`: Just the important columns from the above Companies House data (created by running `companies_house_processing.py`).
- `atmosphericemissionsghg.xlsx`: The total greenhouse gas emissions by SIC from the ONS website [here](https://www.ons.gov.uk/economy/environmentalaccounts/datasets/ukenvironmentalaccountsatmosphericemissionsgreenhousegasemissionsbyeconomicsectorandgasunitedkingdom). The current edition of the dataset was downloaded as of 02/05/23 and uploaded to S3 (`s3://prinz-green-jobs/inputs/data/industry_data/atmosphericemissionsghg.xlsx`).
- `atmosphericemissionsghgintensity.xlsx`: The greenhouse gas emissions per unit of economic output by SIC from the ONS website [here](https://www.ons.gov.uk/economy/environmentalaccounts/datasets/ukenvironmentalaccountsatmosphericemissionsgreenhousegasemissionsintensitybyeconomicsectorunitedkingdom/current). The most current version of the dataset was downloaded on 13/06/2023 and uploaded to S3 (`s3://prinz-green-jobs/inputs/data/industry_data/atmosphericemissionsghgintensity.xlsx`).
- `publisheduksicsummaryofstructureworksheet.xlsx`: The ONS SIC codes downloaded from [here](https://www.ons.gov.uk/methodology/classificationsandstandards/ukstandardindustrialclassificationofeconomicactivities/uksic2007). The 'UK SIC 2007 Summary of Structure Worksheet' xlsx was downloaded on 02/05/2023 and uploaded to S3 (`s3://prinz-green-jobs/inputs/data/industry_data/publisheduksicsummaryofstructureworksheet.xlsx`).
- `greentasks.xlsx`: The ONS time spent on green tasks from [here](https://www.ons.gov.uk/economy/environmentalaccounts/datasets/timespentongreentasks) - downloaded on 12/06/2023 and uploaded to S3 (`s3://prinz-green-jobs/inputs/data/industry_data/greentasks.xlsx`). "Experimental estimates of the time spent doing green tasks, over time, by UK country and by industry. The estimates use a new method based on task-level data from the ONET database in the US."

## Green measures

Measures based of the job adverts predicted SIC:

- INDUSTRY TOTAL GHG EMISSIONS : The 2020 total greenhouse gas emissions for this SIC section code (letter) or division code (two digit). From `atmosphericemissionsghg.xlsx`.
- INDUSTRY GHG PER UNIT EMISSIONS : The 2021 greenhouse gas emissions per unit of economic activity for this SIC section code (letter) or division code (two digit). From `atmosphericemissionsghgintensity.xlsx`.
- INDUSTRY PROP HOURS GREEN TASKS : The 2019 proportion of hours worked spent doing green tasks for this SIC section code (letter). From `greentasks.xlsx`.
- INDUSTRY PROP WORKERS GREEN TASKS : The 2019 proportion of workers doing green tasks for this SIC section code (letter). From `greentasks.xlsx`.
- INDUSTRY PROP WORKERS 20PERC GREEN TASKS : Thw 2019 proportion of workers spending at least 20% of their time doing green tasks for this SIC section code (letter). From `greentasks.xlsx`.

## Usage

```
from dap_prinz_green_jobs.pipeline.green_measures.industries.industries_measures_utils import IndustryMeasures
im = IndustryMeasures()
im.load_ch()
im.get_green_measure_for_company("Global British Petroleum")
>>> {'SIC': '09100', 'SIC_name': 'Support activities for petroleum and natural gas extraction', 'INDUSTRY TOTAL GHG EMISSIONS': 28.2, 'INDUSTRY GHG PER UNIT EMISSIONS': 0.05, 'INDUSTRY PROP HOURS GREEN TASKS': 17, 'INDUSTRY PROP WORKERS GREEN TASKS': 85.7, 'INDUSTRY PROP WORKERS 20PERC GREEN TASKS': 22.5}
im.get_green_measure_for_company("Boots")
>>> {'SIC': '46450', 'SIC_name': 'Wholesale of perfume and cosmetics', 'INDUSTRY TOTAL GHG EMISSIONS': 6299.6, 'INDUSTRY GHG PER UNIT EMISSIONS': 0.11, 'INDUSTRY PROP HOURS GREEN TASKS': 9.4, 'INDUSTRY PROP WORKERS GREEN TASKS': 39.7, 'INDUSTRY PROP WORKERS 20PERC GREEN TASKS': 17.4}
```
