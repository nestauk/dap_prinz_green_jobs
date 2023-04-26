# Green Skills

This directory contains the scripts needed to extract green skills for a given job advert.

As a baseline, we are using the `custom usage` feature of the Skills Extractor library to extract and map skills onto a structured green skills list.

## Green Skills List formatting

You will need to format the green skills list to be in a structure compatible with the Skills Extractor library.

### ESCO

To format the ESCO green skills taxonomy, run the following:

`python dap_prinz_green_jobs/pipeline/green_skills/green_esco_formatting.py`

As a one off, to embed the taxonomy so the extract green skills flow in `ojo_application` is faster:

`python dap_prinz_green_jobs/pipeline/green_skills/green_taxonomy_embedding.py --config_name "extract_green_skills_esco.yaml"`
