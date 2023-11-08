# 🥬 Green Measures

This directory is organised as follows:

1. `industries/`: Scripts, methods and classes to extract green measures at the industry- level.
2. `occupations/`: Scripts, methods and classes to extract green measures at the occupation- level.
3. `skills/`: Scripts, methods and classes to extract green measures at the skill- level.

To extract measures at all levels of granularity, you may use the `GreenMeasures` class in `green_measures.py`:

```
from dap_prinz_green_jobs.pipeline.green_measures.green_measures import GreenMeasures

job_ad = {'id': 1,
 'title': 'Senior Sustainability Consultant',
 'job_text': 'You will work as part of a peer group of specialists and project managers, supported by a strong and diverse team of consultants and senior leaders. We are a organisation that is part of the architecture sector and is focused on the build environment. The role requires strong skills in sustainability reporting and knowledge of climate change. It also requires a sound understanding of qualitative/quantitative analysis and excellent report writing and communication skills.'}

gm = GreenMeasures() #instantiate class
measures = gm.get_green_measures(job_ad) #Extract measures at all levels of granularity

>> {'SKILL MEASURES': {1: {'NUM_ORIG_ENTS': 6,
   'NUM_SPLIT_ENTS': 7,
   'ENTS': [(['sustainability reporting'], 'SKILL'),
    (['knowledge of climate change'], 'SKILL'),
    (['understanding of qualitative quantitative analysis'], 'SKILL'),
    (['report writing'], 'SKILL'),
    (['communication skills'], 'SKILL'),
    (['work as part of a peer group of specialists and',
      'peer group of specialists and project managers'],
     'MULTISKILL')],
   'GREEN_ENTS': [('sustainability reporting',
     ('green',
      1.0,
      ('sustainability',
       'b1b118c4-3291-484e-b64d-6d51fd5da8b3',
       0.7539591423676308))),
    ('knowledge of climate change',
     ('green',
      0.976,
      ('nature of climate change impact',
       '1565b401-1754-4b07-8f1a-eb5869e64d95',
       0.7173026456439915)))],
   'PROP_GREEN': 0.2857142857142857,
   'BENEFITS': None}},
 'INDUSTRY MEASURES': {1: {'SIC': '711',
   'SIC_name': 'Architectural and engineering activities and related technical consultancy',
   'SIC_confidence': 0.73,
   'SIC_method': 'closest distance',
   'company_description': 'We are a organisation that is part of the architecture sector and is focused on the build environment.',
   'INDUSTRY TOTAL GHG EMISSIONS': 297.9,
   'INDUSTRY GHG PER UNIT EMISSIONS': 0.02,
   'INDUSTRY PROP HOURS GREEN TASKS': 11.4,
   'INDUSTRY PROP WORKERS GREEN TASKS': 50.2,
   'INDUSTRY PROP WORKERS 20PERC GREEN TASKS': 26.6,
   'INDUSTRY GHG EMISSIONS PER EMPLOYEE': 0.7,
   'INDUSTRY CARBON DIOXIDE EMISSIONS PER EMPLOYEE': 1709.4}},
 'OCCUPATION MEASURES': {1: {'GREEN CATEGORY': 'Green New & Emerging',
   'GREEN/NOT GREEN': 'Green',
   'GREEN TIMESHARE': 62.5,
   'GREEN TOPICS': 55,
   'SOC': {'SOC_2020_EXT': '2152/05',
    'SOC_2020': '2152',
    'SOC_2010': '2142',
    'name': ['Environment professionals',
     'Environmental and geo-environmental engineers',
     'Sustainability officers',
     'Environmental scientists',
     'Energy managers']}}}}
```
