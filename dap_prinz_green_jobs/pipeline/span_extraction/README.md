# Span Extraction

## Generate training data

To generate training data, run the following:

```
python -m dap_prinz_green_jobs.pipeline.span_extraction.make_training_data --train_size 5000
```

This will load the mixed ojo sample of likely green job adverts and random job adverts, minimally clean the job advert texts and saves data as .jsonl of train_size size (in the command above, 5000 job adverts) to s3.

If you would like to save the training data locally, pass `--local` when running the script.

## Run Prodigy LLM for NER labelling locally

### Setting up your environment and environmental variables

To avoid any environment conflicts, it would be best to create a new prodigy environment, [install Prodigy](https://prodi.gy/docs/install) in your prodigy environment and a few additional Python dependencies:

```bash
conda create --name prodigy_env pip python=3.8
conda activate prodigy_env
python -m pip install prodigy -f https://XXXX-XXXX-XXXX-XXXX@download.prodi.gy
python -m pip install -r prodigy_requirements.txt
```

Create a .env file in your directory root and add your openAI keys:

```
OPENAI_ORG = "org-"
OPENAI_KEY = "sk-"
```

### Downloading data locally

If you would like to pass examples with manually extracted company description, company sector, qualification, skill, multiskill and company benefit, you will first need to download the ner_ojo.yml file locally:

```
#download the examples yaml file locally
aws s3 cp s3://prinz-green-jobs/inputs/data/training_data/ner_ojo.yml dap_prinz_green_jobs/pipeline/span_extraction/examples/ner_ojo.yml

#download training data of train_size 5000
aws s3 cp s3://prinz-green-jobs/inputs/data/training_data/mixed_ojo_sample_5000.jsonl dap_prinz_green_jobs/pipeline/span_extraction/data/mixed_ojo_sample_5000.jsonl
```

### Running prodigy recipes

In your prodigy environment with installed prodigy, run:

```
cd dap_prinz_green_jobs/pipeline/span_extraction
python -m prodigy ner.openai.correct mixed_job_sample ./data/mixed_ojo_sample_5000.jsonl "company description,company sector,qualification,skill,multiskill,company benefit" -p ./prompts/ner_prompt.jinja2 -F ./recipes/openai_ner.py
```

If you would like to pass examples via the prompt, you can do so by:

```
cd dap_prinz_green_jobs/pipeline/span_extraction
python -m prodigy ner.openai.correct mixed_job_sample ./data/mixed_ojo_sample_5000.jsonl "company description,company sector,qualification,skill,multiskill,company benefit" -p ./prompts/ner_prompt.jinja2 -e ./examples/ner_ojo.yml -F ./recipes/openai_ner.py
```

In any case, once you modify the labelling to make it correct, you can 'flag' the corrected labels to tune the prompt.
