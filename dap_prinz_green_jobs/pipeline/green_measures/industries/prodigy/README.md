# Training data generation for company descriptions & SIC codes

## Generate training data

To generate training data, run the following:

```
python dap_prinz_green_jobs/pipeline/green_measures/industries/prodigy/make_training_data.py --train_size 5000
```

This will load the mixed ojo sample of likely green job adverts and random job adverts, minimally clean the job advert texts and saves data as .jsonl of train_size size (in the command above, 5000 job adverts) to s3.

You can also pass a `--random_seed` argument to set a different random seed. This is useful if you want to generate a different sample of job adverts.

## Run prodigy recipes

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
OPENAI_API_KEY = "sk-"
```

### Downloading data locally

To download i.e. `mixed_ojo_sample_5000.jsonl` run:

```
#download training data of train_size 5000
aws s3 cp s3://prinz-green-jobs/inputs/data/training_data/mixed_ojo_sample_5000.jsonl dap_prinz_green_jobs/pipeline/green_measures/industries/prodigy/data/mixed_ojo_sample_5000.jsonl
```

#### Running custom recipe

In your prodigy environment with installed prodigy, run:

```
prodigy oa_ner_classification comp_sic_annotated \
    dap_prinz_green_jobs/pipeline/green_measures/industries/prodigy/data/mixed_ojo_sample_5000.jsonl \
    -F dap_prinz_green_jobs/pipeline/green_measures/industries/prodigy/custom_openai_recipe.py
```

This runs a custom prodigy recipe that uses gpt 3.5 to extract a company description phrase and the SIC code based on the description phrase. You are able to modify the phrase.

**NOTE:** Do modify phrases that do not match with gpt 3.5's prediction or that seem nonsensical. The span start and ends sometimes highlight incorrect phrases so be conscious of this.

If the SIC code is incorrect, you can select the 'wrong SIC code' option. Do **NOT** select the SIC company description phrase.

To save the outputs of the labelled data:

```
prodigy db-out comp_sic_annotated > dap_prinz_green_jobs/pipeline/green_measures/industries/prodigy/data/company_desc_sic_labelled.jsonl
aws s3 cp dap_prinz_green_jobs/pipeline/green_measures/industries/prodigy/data/company_desc_sic_labelled.jsonl s3://prinz-green-jobs/outputs/data/labelled_job_adverts/company_desc_sic_labelled.jsonl
```

**NOTE:** You must provide the session url argument (with your name) when labelling the tasks if this is hosted on EC2, e.g. http://18.XXX:8080/?session=liz. This makes it so no two labellers will end up annotating the same task. Without it each time someone tried to label the stream of tasks will be exactly the same as another labeller.

## Saving and converting labelled data

To download the .jsonl training data:

```
aws s3 cp s3://prinz-green-jobs/outputs/data/labelled_job_adverts/company_desc_sic_labelled.jsonl dap_prinz_green_jobs/pipeline/green_measures/industries/prodigy/data/labelled_data/company_desc_sic_labelled.jsonl
```

For binary classification downstream, you need to convert the .jsonl training data to sentences and labels:

```
python dap_prinz_green_jobs/pipeline/green_measures/industries/prodigy/convert_training_data.py -f company_desc_sic_labelled.jsonl
```

If you would like to save the labelled data locally instead, pass a `-l` flag.
