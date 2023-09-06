# Green Skills

This directory contains the scripts needed to extract green skills for a given job advert.

The script

```
dap_prinz_green_jobs/pipeline/green_measures/skills/skills_measures_utils.py
```

contains the main functions needed to extract green skills. As a baseline, we are using the `custom usage` feature of the Skills Extractor library to extract and map skills onto a structured green skills list.

## Green Skills List formatting

You will need to format the green skills list to be in a structure compatible with the Skills Extractor library.

### ESCO formatted data

To format the ESCO green skills taxonomy, run the following as a one off:

`python dap_prinz_green_jobs/pipeline/green_measures/skills/green_esco_formatting.py`

### Custom config

Note: this isn't neccessary to run anymore.

To add the custom config file, formatted skills taxonomy and taxonomy embeddings to the relevant location in the `ojd-daps-skills` library location:

```
python dap_prinz_green_jobs/pipeline/green_measures/skills/customise_skills_extractor.py --config_name "extract_green_skills_esco"
```

## Datasets used

- `greenSkillsCollection_en.csv`: A dataset of ESCO's green skills as downloaded on the 24th April 2023. This is stored on S3 [here](`s3://prinz-green-jobs/inputs/data/green_skill_lists/esco/greenSkillsCollection_en.csv`).
- `esco_data_formatted.csv`: The formatted version of ESCO's full skills taxonomy as downloaded from July 2022. This was formatted by running [this script from the ojd_daps_skills repo](https://github.com/nestauk/ojd_daps_skills/blob/dev/ojd_daps_skills/pipeline/skill_ner_mapping/esco_formatting.py).

## Green measures

- NUM_ENTS: The total number of entities found in this job advert
- ENTS: The predicted entities
- ENT_TYPES: Which entity type each entity corresponds to (usually SKILL or EXPERIENCE)
- GREEN_ENTS: The green skills entities are mapped to (if any). In the format [(entity_as_in_advert, (esco_green_skill_name, esco_green_skill_id)),].
- PROP_GREEN: The proportion of entities which were mapped to green skills (len(GREEN_ENTS)/NUM_ENTS)

## Usage

Note: predicting skill entities and embedding them for comparison with the green skills taxonomy can take a long time if you are inputting many job adverts.

```
from dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils import SkillMeasures

sm = SkillMeasures(config_name="extract_green_skills_esco")
sm.initiate_extract_skills(local=False, verbose=True)

job_adverts = [{"id": 1, "job_text": "This job requires communication and Excel skills. We want someone with experience in sustainability. Heat pump installation skills would be useful. We have a pension scheme benefit."}]

# We will predict entities using our NER model
predicted_entities = sm.get_entities(job_adverts)

# this gives {1: {'SKILL': ['communication', 'Excel', 'Heat pump installation skills'], 'MULTISKILL': [], 'EXPERIENCE': ['experience in sustainability'], 'BENEFIT': ['pension scheme']}}

# Just take the predicted entities which are to do with skills
skills_list = []
for p in predicted_entities.values():
    for ent_type in ["SKILL", "MULTISKILL", "EXPERIENCE"]:
        for skill in p[ent_type]:
            skills_list.append(skill)

# Embed these skills
all_extracted_skills_embeddings_dict = sm.get_skill_embeddings(list(set(skills_list)))

# We will load the green taxonomy embeddings from S3 since they have already been calculated
taxonomy_skills_embeddings_dict = sm.get_green_taxonomy_embeddings(output_path="outputs/data/green_skill_lists/green_esco_embeddings_20230815.json", load=True)

# Map the newly extracted skills to the green skills taxonomy

all_extracted_green_skills_dict = sm.map_green_skills()

prop_green_skills = sm.get_measures(
    job_advert_ids=[j["id"] for j in job_adverts],
    predicted_entities=predicted_entities,
    all_extracted_green_skills_dict=all_extracted_green_skills_dict,
)

prop_green_skills
>>> {'1': {'NUM_ENTS': 4, 'ENTS': ['communication', 'Excel', 'Heat pump installation skills', 'experience in sustainability'], 'ENT_TYPES': ['SKILL', 'SKILL', 'SKILL', 'EXPERIENCE'], 'GREEN_ENTS': [('Heat pump installation skills', ('heat pump installation', 2)), ('experience in sustainability', ('sustainability', 1))], 'PROP_GREEN': 0.5}}

```
