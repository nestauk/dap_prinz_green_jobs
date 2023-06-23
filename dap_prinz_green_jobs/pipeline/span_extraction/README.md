## Span Extraction

### Generate training data

To generate training data, run the following at the root of the directory:

```
python -m dap_prinz_green_jobs.pipeline.span_extraction.make_training_data --train_size 5000
```

This will load the mixed ojo sample of likely green job adverts and random job adverts, minimally clean the job advert texts and saves data as .jsonl of train_size size (in the command above, 5000 job adverts) to s3.

If you would like to save the training data locally, pass `--local True` when running the script. If you want to save the training examples to pass through the prompt locally, pass `--examples True` when running the script.

### Run prodigy locally

(Assuming you're in this directory and you've saved both the training data and examples locally)

Create a .env file in your directory root and add your openAI keys:

```
OPENAI_ORG = "org-"
OPENAI_KEY = "sk-"
```

Run:

```
python -m prodigy ner.openai.correct mixed_job_sample ./data/mixed_ojo_sample_5000.jsonl "company description,company sector,qualification,skill,multiskill,company benefit" -p ./prompts/ner_prompt.jinja2 -e ./examples/ner_ojo.yml -n 2 -F ./recipes/openai_ner.py
```

If you do not wish to pass examples via the prompt, run:

```
python -m prodigy ner.openai.correct mixed_job_sample ./data/mixed_ojo_sample_5000.jsonl "company description,company sector,qualification,skill,multiskill,company benefit" -p ./prompts/ner_prompt.jinja2 -F ./recipes/openai_ner.py
```
