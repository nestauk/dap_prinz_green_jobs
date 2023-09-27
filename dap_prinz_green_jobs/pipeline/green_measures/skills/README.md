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

### Green skills classifier

To train the green skills random forest classifier run:

```
python dap_prinz_green_jobs/pipeline/green_measures/skills/green_skill_classifier.py

```

The training data for this is `s3://prinz-green-jobs/inputs/data/training_data/green_skill_training_data.csv` - this was created by labelling a dataset of frequently occurring skills, as well as skills which are mapped to ESCO green skills with a high similarity score.

This dataset contains 971 skills labelled as not-green and 743 labelled as green.

The most recently saved model `s3://prinz-green-jobs/outputs/models/green_skill_classifier/green_skill_classifier_20230906.joblib` has the following test metrics:

```
              precision    recall  f1-score   support

       green       0.90      0.86      0.88       175
   not_green       0.91      0.94      0.92       254

    accuracy                           0.91       429
   macro avg       0.91      0.90      0.90       429
weighted avg       0.91      0.91      0.91       429

```

This trained model can be loaded and used by running:

```python
from dap_prinz_green_jobs.pipeline.green_measures.skills.green_skill_classifier import GreenSkillClassifier

green_skills_classifier = GreenSkillClassifier()
green_skills_classifier.load_esco_data()
green_skills_classifier.load(
    model_file="s3://prinz-green-jobs/outputs/models/green_skill_classifier/green_skill_classifier_20230906.joblib"
)

pred_green_skill = green_skills_classifier.predict(
    ["Excel skills", "Heat pump installation skills"]
)

>>> [('not_green', 1.0, ('carry out sample analysis', '82423b5c-486f-42e7-b00e-7358757a8de5', 0.2772792296638087)), ('green', 0.7191159533073931, ('heat pump installation', '00735755-adc6-4ea0-b034-b8caff339c9f', 0.9072619656040537))]

```

As you can see, the closest green ESCO skill will always be outputted, even if the skill have been predicted as "not_green".

## Datasets used

- `greenSkillsCollection_en.csv`: A dataset of ESCO's green skills as downloaded on the 24th April 2023. This is stored on S3 [here](`s3://prinz-green-jobs/inputs/data/green_skill_lists/esco/greenSkillsCollection_en.csv`).
- `esco_data_formatted.csv`: The formatted version of ESCO's full skills taxonomy as downloaded from July 2022. This was formatted by running [this script from the ojd_daps_skills repo](https://github.com/nestauk/ojd_daps_skills/blob/dev/ojd_daps_skills/pipeline/skill_ner_mapping/esco_formatting.py).

## Green measures

- NUM_ORIG_ENTS: The total number of entities found in this job advert
- NUM_SPLIT_ENTS: The number of entities after splitting up long ones
- ENTS: The predicted entities. In the format `[(['communication'], 'SKILL'), (['this skill', 'was split up'], 'SKILL')]`
- GREEN_ENTS: The green skills entities are mapped to (if any). In the format `[(entity, (green/not-green, probability of green/not-green, (most similar ESCO green skill, most similar ESCO green skill ID, most similar ESCO green skill similarity score)))]` for example `[('Heat pump installation skills', ('green', 0.72, ('heat pump installation', '00735755-adc6-4ea0-b034-b8caff339c9f', 0.91)))]`.
- PROP_GREEN: The proportion of entities which were mapped to green skills (len(GREEN_ENTS)/NUM_SPLIT_ENTS)
- BENEFITS: The list of all benefit entities predicted for this job

## Usage

Note: predicting skill entities and embedding them for comparison with the green skills taxonomy can take a long time if you are inputting many job adverts.

```python

from dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils import (
    SkillMeasures,
)
sm = SkillMeasures(config_name="extract_green_skills_esco")
sm.initiate_extract_skills(local=False, verbose=True)

job_adverts = [
    {"id": 1, "job_text": "This job requires communication and Excel skills. We want someone with experience in sustainability. Heat pump installation skills would be useful. We have a pension scheme benefit."},
    {"id": 55, "job_text": "This job contains no skills."},
    {"id": 3, "job_text": "This job contains Excel skills."},
    {"id": 8, "job_text": "This job contains a really long sentence. Promote good practice of material sustainability (reuse and or recycle) initiatives to reduce waste and save costs."},
    ]

# We will load the green taxonomy embeddings from S3 since they have already been calculated
taxonomy_skills_embeddings_dict = sm.get_green_taxonomy_embeddings(
    output_path="outputs/data/green_skill_lists/green_esco_embeddings_20230815.json", load=True)

prop_green_skills = sm.get_measures(job_adverts)

# {
# 1: {'NUM_ORIG_ENTS': 4, 'NUM_SPLIT_ENTS': 4, 'ENTS': [(['communication'], 'SKILL'), (['Excel'], 'SKILL'), (['Heat pump installation skills'], 'SKILL'), (['experience in sustainability'], 'EXPERIENCE')], 'GREEN_ENTS': [('Heat pump installation skills', ('green', 0.7191159533073931, ('heat pump installation', '00735755-adc6-4ea0-b034-b8caff339c9f', 0.9072619656040537))), ('experience in sustainability', ('green', 0.992, ('sustainability', 'b1b118c4-3291-484e-b64d-6d51fd5da8b3', 0.7593304913709202)))], 'PROP_GREEN': 0.5, 'BENEFITS': None},
# 55: {'NUM_ORIG_ENTS': 0, 'NUM_SPLIT_ENTS': 0, 'ENTS': None, 'GREEN_ENTS': None, 'PROP_GREEN': 0, 'BENEFITS': None},
#  3: {'NUM_ORIG_ENTS': 1, 'NUM_SPLIT_ENTS': 1, 'ENTS': [(['Excel'], 'SKILL')], 'GREEN_ENTS': [], 'PROP_GREEN': 0.0, 'BENEFITS': None},
#  8: {'NUM_ORIG_ENTS': 1, 'NUM_SPLIT_ENTS': 3, 'ENTS': [(['Promote good practice of material sustainability (reuse and or recycle)', 'sustainability (reuse and or recycle) initiatives to reduce waste and', 'initiatives to reduce waste and save costs'], 'MULTISKILL')], 'GREEN_ENTS': [('Promote good practice of material sustainability (reuse and or recycle)', ('green', 0.982, ('promote sustainability', '469e19ed-a0bd-445a-ae2d-4ba9430e296b', 0.7094095115887558))), ('sustainability (reuse and or recycle) initiatives to reduce waste and', ('green', 1.0, ('analyse  new recycling opportunities', '89f5fa96-ae45-4906-902c-d50ca51009c6', 0.7682701732964252))), ('initiatives to reduce waste and save costs', ('green', 0.828, ('managing waste', '40f65a56-ccbe-4601-9f32-1cc6cdd24f28', 0.7851462257390638)))], 'PROP_GREEN': 1.0, 'BENEFITS': None}}

```
