# Extracting green skills from job adverts

This directory contains the scripts needed to extract green skills from job adverts.

As a baseline, we are using the `custom usage` feature of the Skills Extractor library to extract and map skills onto a structured green skills list.

## Green Skills List formatting

You will need to format the green skills list to be in a structure compatible with the Skills Extractor library.

### ESCO

To format the ESCO green skills taxonomy, run the following:

`python dap_prinz_green_jobs/pipeline/green_skills/green_esco_formatting.py`

As a one off, to embed the taxonomy so the extract green skills flow is faster:

`python dap_prinz_green_jobs/pipeline/green_skills/green_taxonomy_embedding.py --config_name "extract_green_skills_esco.yaml"`

## Extract Green Skills Pipeline

To extract skills and map them onto ESCO's green skills taxonomy, you can run the following flow:

`python dap_prinz_green_jobs/pipeline/green_skills/extract_green_skills_flow.py`

The flow takes a number of parameters that will need to be changed, depending on where in your system the `ojd-daps-skills` library is.

Namely, you will need to change `extract_skills_library_path` to accomodate for moving the custom config file, the formatted green skills list and the green taxonomy embeddings to the relevant location.
