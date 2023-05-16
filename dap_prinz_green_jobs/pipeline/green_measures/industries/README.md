# Industry Green Measures

As a one-off run:

```
python dap_prinz_green_jobs/pipeline/green_measures/industries/industries_data_processing.py

```

this will save out just the key columns from the full Companies House dataset to `s3://prinz-green-jobs/inputs/data/industry_data/BasicCompanyDataAsOneFile-2023-05-01_key_columns_only.csv`. This speeds up the rest of the pipeline as loading the full dataset is slow. It will also save a series of relevant GHG data.

The script

```
dap_prinz_green_jobs/pipeline/green_measures/industries/industry_measures_utils.py
```

contains functions needed to see whether industries are green or not.

## Datasets used

- `BasicCompanyDataAsOneFile-2023-05-01.csv`: A snapshot of the Companies House dataset was downloaded from the gov website [here](http://download.companieshouse.gov.uk/en_output.html). The "BasicCompanyDataAsOneFile-2023-05-01.zip (439Mb)" file was downloaded and unzipped as of 02/05/2023 and then uploaded to S3 (`s3://prinz-green-jobs/inputs/data/industry_data/BasicCompanyDataAsOneFile-2023-05-01.csv`).
- `BasicCompanyDataAsOneFile-2023-05-01_key_columns_only.csv`: Just the important columns from the above Companies House data (created by running `companies_house_processing.py`).
- `atmosphericemissionsghg.xlsx`: The greenhouse gas emissions per SIC from the ONS website [here](https://www.ons.gov.uk/economy/environmentalaccounts/datasets/ukenvironmentalaccountsatmosphericemissionsgreenhousegasemissionsbyeconomicsectorandgasunitedkingdom). The current edition of the dataset was downloaded as of 02/05/23 and uploaded to S3 (`s3://prinz-green-jobs/inputs/data/industry_data/atmosphericemissionsghg.xlsx`).
- `publisheduksicsummaryofstructureworksheet.xlsx`: The ONS SIC codes downloaded from [here](https://www.ons.gov.uk/methodology/classificationsandstandards/ukstandardindustrialclassificationofeconomicactivities/uksic2007). The 'UK SIC 2007 Summary of Structure Worksheet' xlsx was downloaded on 02/05/2023 and uploaded to S3 (`s3://prinz-green-jobs/inputs/data/industry_data/publisheduksicsummaryofstructureworksheet.xlsx`).
