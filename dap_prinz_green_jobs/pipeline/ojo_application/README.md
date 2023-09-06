# OJO Application

This directory will contain scripts to run/apply green measures on OJO samples.

It will also contain scripts to generate an OJO sample.

Ultimately, we would like to have a single flow that applies the green measures class to a representative OJO sample to be used for i.e. a dashboard.

## Extract Green Measures

To extract measures across the skill-, occupations- and industries- level, you can run the following:

```
python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_measures.py --config_name "base" --production
```

This will save out three files, one for each of the measures.

### Loading pre-calculated data and the config file

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
