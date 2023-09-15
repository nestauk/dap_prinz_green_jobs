# SIC Mapper

## Synthetic SIC data generation & post processing

To generate a dataset of SIC codes described as company descriptions for downstream mapping, run as a one off:

```
export OPENAI_API_KEY="sk-xxx" #expore your openAI key in your terminal
python dap_prinz_green_jobs/pipeline/green_measures/industries/sic_mapper/sic_data_generation.py
```

This will also create and save a FAISS index of the SIC codes and their descriptions.

Please note that this script takes a long time to run in production. If you would like to run this on all SIC codes, you will need to pass the `--production` flag.

As we are using an LLM to generate company descriptions of SIC codes, results will vary every time you run the script.

**NOTE:** if you hit up against a **502 bad gateway** error and are using an apple silicon machine, you need to also run `bash /Applications/Python*/Install\ Certificates.command` in your terminal to install the necessary certificates.

To generate a FAISS index of the SIC company descriptions, run the following:

```
python dap_prinz_green_jobs/pipeline/green_measures/industries/sic_mapper/sic_data_postprocessing.py
```

## SIC Mapper

## High level overview

### Core functionality

To map job adverts to SIC codes, you can use the `SicMapper` class in `sic_mapper.py`:

```
from dap_prinz_green_jobs.pipeline.green_measures.industries.sic_mapper.sic_mapper import SicMapper

job_ad = {'id': 1, 'company_name': Google, 'job_text:' 'We are looking for a software engineer to join our team. We are a fast growing company in the software engineering industry.'}

sm = SicMapper()
sm.load() # load relevant models, tokenizers and datasets

sic_code = sm.get_sic_code(job_ad) # get SIC codes for job advert

>>  [{'id': '1',
    'company_name': 'Google',
    'job_text': 'We are looking for a software engineer to join our team. We are a fast growing company in the software engineering industry.',
    'company_description': 'We are a fast growing company in the software engineering industry.',
    'sic_code': '582',
    'sic_confidence': 0.7}]
```

### Evaluation

When we labelled company descriptions, we also asked the LLM to assign it to a SIC code. We manually verified whether the match was appropriate or not.

We have labelled **287** job ads with SIC codes.

On that evaluation set, company descriptions are extracted **93.2%** of the time.

We then further verified **85** matches and found…

- **73%** of mapped SIC codes are good or ok

- **27%** of mapped SIC codes are bad

- **49%** of matches were at least the same quality or better than the LLM

- **40%** of matches were worse than the LLM

- In **11%** of cases, both the LLM and the current approach were bad
