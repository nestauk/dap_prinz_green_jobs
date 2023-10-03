# 🎾 OJO Application

This directory contains scripts that rely on access to Nesta's database of job ads. 

The directory is split into two sections:

1. `ojo_sample/` - scripts that generate a sample of job ads from the database. 
2. `extract_green_measures.py` - A script that extract green measures at the skill-, occupations- and industries-level from a sample of job ads.

## 🔧 Extract Green Measures

To extract measures across the skill-, occupations- and industries- level on a sample of job ads, you can run the following:

```
python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_measures_metaflow.py --package-suffixes=.txt --datastore=s3 run
```

```
python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_measures.py --config_name "base" --production
```

This will save out three files, one for each of the measures.

### :sparkles: setting up with AWS and metaflow

If you haven't used batch processing with Metaflow before and want to run any of the flows that make use of batch (e.g. `ojobert_flow.py`), you'll need to ensure a few things are set up first:

1. Your metaflow config file needs be setup with the correct parameters. You can find your config file by executing `metaflow configure show`. If you don't have parameters such as `METAFLOW_ECS_S3_ACCESS_IAM_ROLE` and `METAFLOW_ECS_FARGATE_EXECUTION_ROLE`, contact the DE team.
2. If your laptop username contains a `.` (e.g. if you run `whoami` from the command line and it returns `jack.vines` rather than `jackvines`), you'll need to change your username to remove the `.`. This is because the AWS Batch job will fail to run if your username contains a `.`. To fix this, add `export METAFLOW_USER=<your name without the period>` to a `.env` file at the root of the project. Then, [one time only] run `source .env` to trigger reloading of the variable.

### ⏳ Loading pre-calculated data and the config file

The config file you use should contain the following arguments to load pre-calculated data:

```
load_skills:
load_skills_embeddings:
load_taxonomy_embeddings:
skills_output:
skill_embeddings_output:
green_tax_embedding_path:
```

By loading the skills predicted and the embeddings it greatly speeds up running this script. However, if you are running this script it's probably because something has changed, whether it be the model, the algorithm or the dataset of job adverts you are extracting green measures from. In which case you will need to recalculate most of these files anyway.

You probably won't need to rerun calculating the embeddings for the ESCO green skills taxonomy unless this dataset changes. i.e. So if you need to rerun the green measures you are likely to need to change your config to:

```
load_skills: False
load_skills_embeddings: False
load_taxonomy_embeddings: True
skills_output:
skill_embeddings_output:
green_tax_embedding_path: "outputs/data/green_skill_lists/green_esco_embeddings_20230815.json"

```

If your way of embedding skills changes, but your job advert dataset and the NER model remained the same (and therefore the skill predictions were the same) you could change these to:

```
load_skills: True
load_skills_embeddings: False
load_taxonomy_embeddings: False
skills_output: "outputs/data/green_skill_lists/skills_data_ojo_mixed_20230815.json"
skill_embeddings_output:
green_tax_embedding_path:
```

If your job advert dataset and/or model changes you will always need to set `load_skills: False` and `load_skills_embeddings: False`.