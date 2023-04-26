# OJO Application

This directory will contain scripts to run/apply green measures on OJO samples.

Ultimately, we would like to have a single flow that applies the green measures class to a representative OJO sample to be used for i.e. a dashboard.

## Extract Green Skills Flow

To extract skills from a sample of OJO job adverts and map them onto ESCO's green skills taxonomy, you can run the following flow:

`python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_skills_flow.py`

The flow takes a number of parameters that will need to be changed, depending on where in your system the `ojd-daps-skills` library is.

Namely, you will need to change `extract_skills_library_path` to accomodate for moving the custom config file, the formatted green skills list and the green taxonomy embeddings to the relevant location.
