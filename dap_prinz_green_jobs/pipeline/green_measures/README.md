# Green Measures Library

This directory will be organised such that each subdirectory (`occupations`, `industries`, `skills`) contains any necessary models, processing relevant to the different green measure types.

Scripts that rely on OJO data do NOT belong here - they should be in the `ojo_application/` directory.

There will be a script called `green_measures.py` that will be a class which takes as input a job advert and outputs green measures at the industry, occupation and skill level.

It will have methods to i.e. `green_measures.get_skill_measures(job_advert)`, `green_measures.get_industry_measures(job_advert)`, `green_measures.get_occupation_measures(job_advert)`.
