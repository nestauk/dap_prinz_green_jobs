# 🥬 Green Measures

This directory is organised as follows:

1. `industries/`: Scripts, methods and classes to extract green measures at the industry- level.
2. `occupations/`: Scripts, methods and classes to extract green measures at the occupation- level.
3. `skills/`: Scripts, methods and classes to extract green measures at the skill- level.

To extract measures at all levels of granularity, you may use the `GreenMeasures` class in `green_measures.py`:

```
from dap_prinz_green_jobs.pipeline.green_measures.green_measures import GreenMeasures

job_ad = {"id": 1,
"company_name": "Test Company A",
"title": "Data Scientist",
"job_text": "We are looking for a data scientist to join our team. We are a manufacturing company. You should be able to use python and R. You should also have experience in sustainability."}

gm = GreenMeasures() #instantiate class
measures = gm.get_green_measures(job_ad) #Extract measures at all levels of granularity
```
