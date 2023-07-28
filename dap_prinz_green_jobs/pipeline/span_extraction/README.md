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
python -m pip install -r prodigy_requirements.txt #install additional langchain and openai libraries
```

Create a .env file in your directory and add your openAI key:

```
OPENAI_KEY = "sk-"
```

### Downloading data locally

```
#download training data of train_size 5000
aws s3 cp s3://prinz-green-jobs/inputs/data/training_data/mixed_ojo_sample_5000.jsonl dap_prinz_green_jobs/pipeline/span_extraction/data/mixed_ojo_sample_5000.jsonl
```

### Running prodigy recipes

In your prodigy environment with installed prodigy, run:

```
prodigy oa_ner_classification comp_desc_annotated \
    dap_prinz_green_jobs/pipeline/span_extraction/data/mixed_ojo_sample_5000.jsonl \
    -F dap_prinz_green_jobs/pipeline/span_extraction/custom_openai_recipe.py
```

This runs a custom prodigy recipe that uses gpt 3.5 to predict the SIC code and the phrase most predictive of the SIC code. You are able to modify both the phrase and the SIC code. If the SIC code is incorrect, you can update it by:

1. selecting the 'wrong SIC code' option;
2. writing the correct SIC code in the free text box.
