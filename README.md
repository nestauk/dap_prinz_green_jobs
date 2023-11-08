# 🥬 Measuring Green Jobs

This repository contains the code required to measure the greeness of job adverts at the skill-, occupation- and sector-level. At it's highest level, this codebase contains the algorithms required to:

1. **Extract** relevant information from job ads;
2. **Map** information to UK government standards including Standard Occupation Classification (SOC) codes, Standard Industrial Classification (SIC) codes and the European Skills, Competences, Qualifications and Occupations (ESCO) Green Skills taxonomy;
3. **Join** standards to publically available greeness datasets, such as industry-level GHG emissions as reported by the UK's Office for National Statistics or the U.S. Bureau of Labor Statistics's Occupational Information Network (O\*NET)'s occupation-level time spent on green tasks.

At the job advert level, this can be summaried with the following visual:

<p align="center">
  <img src="https://github.com/nestauk/dap_prinz_green_jobs/assets/46863334/db07e584-c1cc-476e-8ee1-050562318daf" />
</p>

## TL;DR: Extracting green measures

To extract green measures at the job advert level, you can use the `GreenMeasures` class to extract measures at the skill-, occupation- and sector-level. The following code snippet shows how to extract measures from a single job advert:

```
from dap_prinz_green_jobs.pipeline.green_measures.green_measures import GreenMeasures

job_ad = {'id': 1,
 'job_title': 'Senior Sustainability Consultant',
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

You can also pass a list of job adverts to the `get_green_measures` method to extract measures from multiple job adverts at once.

Should you like to extract a single measure (i.e. extract green skills) or if you would just like to extract SOC, SIC or skills, please refer to detailed READMEs in the `dap_prinz_green_jobs/pipeline/green_measures/` directory.

## Project structure

Core to the codebase are the following directories:

- `dap_prinz_green_jobs/pipeline/green_measures/`: This directory contains the code required and methodological summaries to **extract and map** job adverts to:

  - `occupations`: Standard Occupational Classification codes (SOC);
  - `industries`: Standard Industrial Classification codes (SIC) and;
  - `skills`: The European Skills, Competences, Qualifications and Occupations (ESCO) Green Skills taxonomy.

  It also contains code to join those standards to publically available datasets on occupations and sectors.

- `dap_prinz_green_jobs/pipeline/ojo_application`: This directory contains the code required to apply the alorithms on different samples of scraped online job adverts from the [Open Jobs Observatory (OJO)](https://www.nesta.org.uk/data-visualisation-and-interactive/open-jobs-observatory/). As part of the project, Nesta has been scraping online job adverts since 2021 and building algorithms to extract and structure information as part of the Open Jobs Observatory project. Code in this directory requires access to Nesta's private S3 bucket and is not available to the public.

- `dap_prinz_green_jobs/analysis/`: This directory contains the code that powers the [Green Jobs Explorer](https://green-jobs-19776304fc2f.herokuapp.com/occupations), our demo tool to explore and learn more about green jobs and skills.

## Green Jobs Explorer

If you would to explore the data via a front end, we've build a demo tool for reresearchers to explore and learn more about green jobs and skills.

## Setup

- Meet the data science cookiecutter [requirements](http://nestauk.github.io/ds-cookiecutter/quickstart), in brief:
  - Install: `direnv` and `conda`
- Run `make install` to configure the development environment:
  - Setup the conda environment
  - Configure `pre-commit`
- run `python -m spacy download en_core_web_sm`
- run `conda install -c pytorch faiss-cpu=1.7.4 mkl=2021 blas=1.0=mkl` in order to install faiss and its associated dependencies.

## Contributor guidelines

[Technical and working style guidelines](https://github.com/nestauk/ds-cookiecutter/blob/master/GUIDELINES.md)

---

<small><p>Project based on <a target="_blank" href="https://github.com/nestauk/ds-cookiecutter">Nesta's data science project template</a>
(<a href="http://nestauk.github.io/ds-cookiecutter">Read the docs here</a>).
</small>
