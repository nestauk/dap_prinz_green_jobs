## SIC Mapper

Finally, the generate a dataset of SIC codes described as company descriptions for downstream mapping, run as a one off:

```
export OPENAI_API_KEY="sk-xxx" #expore your openAI key in your terminal
python dap_prinz_green_jobs/pipeline/green_measures/industries/sic_mapper/sic_data_processing.py
```

Please note that this script takes a long time to run in production. If you would like to run this on all SIC codes, you will need to pass the `--production` flag.

As we are using an LLM to generate company descriptions of SIC codes, results will vary every time you run the script.
