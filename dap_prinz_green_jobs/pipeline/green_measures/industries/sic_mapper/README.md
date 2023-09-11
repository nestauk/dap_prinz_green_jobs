# SIC Mapper

## SIC data processing

To generate a dataset of SIC codes described as company descriptions for downstream mapping, run as a one off:

```
export OPENAI_API_KEY="sk-xxx" #expore your openAI key in your terminal
python dap_prinz_green_jobs/pipeline/green_measures/industries/sic_mapper/sic_data_processing.py
```

This will also create and save a FAISS index of the SIC codes and their descriptions.

Please note that this script takes a long time to run in production. If you would like to run this on all SIC codes, you will need to pass the `--production` flag.

As we are using an LLM to generate company descriptions of SIC codes, results will vary every time you run the script.

## SIC Mapper

### Core functionality

To map job adverts to SIC codes, you can use the `SicMapper` class in `sic_mapper.py`:

```
from dap_prinz_green_jobs.pipeline.green_measures.industries.sic_mapper.sic_mapper import SicMapper

job_ad = {'id': 1, 'company_name': GreenJobs, 'job_text:' 'We are looking for a data scientist to join our team at Green Jobs.'}

sm = SicMapper()
sm.load() # load relevant models, tokenizers and datasets
sic_code = sm.get_sic_code([job_ad]) # get SIC codes for job adverts
```

### High-level methodological pipeline

### Evaluation
