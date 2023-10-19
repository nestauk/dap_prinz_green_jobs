# 🗺️ SIC Mapper

The high-level goal of the `SicMapper` is to map a given job advert to Standardised Industrial Classification (SIC) code.

## 🔨 Core functionality

To map job adverts to SIC codes, you can use the `SicMapper` class in `sic_mapper.py`:

```
from dap_prinz_green_jobs.pipeline.green_measures.industries.sic_mapper.sic_mapper import SicMapper

job_ad = {'id': 1, 'job_text': 'We are looking for a software engineer to join our team. We are a fast growing company in the software engineering industry.'}

sm = SicMapper()
sm.load() # load relevant models, tokenizers and datasets

sic_code = sm.get_sic_codes(job_ad) # get SIC codes for job advert

>>  [{'id': 1,
  'job_text': 'We are looking for a software engineer to join our team. We are a fast growing company in the software engineering industry.',
  'company_description': 'We are a fast growing company in the software engineering industry',
  'sic_code': '582',
  'sic_name': 'Software publishing',
  'sic_method': 'closest distance',
  'sic_confidence': 0.77}]
```

## 🖊️ Methodology

The SIC Mapper can be described in the following diagram:

<p align="center">
  <img src="https://github.com/nestauk/dap_prinz_green_jobs/assets/46863334/6e16b600-aaa9-46f4-9926-0ad4e772e2ef" />
</p>

## 🤖 Synthetic SIC data generation

As described in the high level diagram, we transform SIC codes to be described as company descriptions as input data in our pipeline.

To generate a dataset of SIC codes described as company descriptions for downstream mapping, run as a one off:

```
export OPENAI_API_KEY="sk-xxx" #expore your openAI key in your terminal
python dap_prinz_green_jobs/pipeline/green_measures/industries/sic_mapper/sic_data_generation.py
```

Please note that this script takes a long time to run in production. If you would like to run this on all SIC codes, you will need to pass the `--production` flag. As we are using an LLM to generate company descriptions of SIC codes, results will vary every time you run the script.

**NOTE:** if you hit up against a **502 bad gateway** error and are using an apple silicon machine, you need to also run `bash /Applications/Python*/Install\ Certificates.command` in your terminal to install the necessary certificates.
