# OJO Application

This directory will contain scripts to run/apply green measures on OJO samples.

It will also contain scripts to generate an OJO sample.

Ultimately, we would like to have a single flow that applies the green measures class to a representative OJO sample to be used for i.e. a dashboard.

## Extract Green Measures

To extract measures across the skill-, occupations- and industries- level, you can run the following:

```
python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_measures.py
```

## Extract Green Skills

To extract skills from a sample of OJO job adverts and map them onto ESCO's green skills taxonomy, you can run the following:

```
python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_skills.py
```

## Extract Green Occupations

To get the green occupation measures for OJO job adverts run:

```
python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_occupations.py
```

## Extract Green Industries

To get the green industry measures for OJO job adverts run:

```
python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_industries.py

```
