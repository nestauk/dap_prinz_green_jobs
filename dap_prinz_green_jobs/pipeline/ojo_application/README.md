# OJO Application

This directory will contain scripts to run/apply green measures on OJO samples.

It will also contain scripts to generate an OJO sample.

Ultimately, we would like to have a single flow that applies the green measures class to a representative OJO sample to be used for i.e. a dashboard.

## Extract Green Skills

To extract skills from a sample of OJO job adverts and map them onto ESCO's green skills taxonomy, you can run the following:

`python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_skills.py`

## Measure Green Occupations

To see which OJO job adverts are for green occupations run:

```
python dap_prinz_green_jobs/pipeline/ojo_application/measure_green_occupations.py

```
