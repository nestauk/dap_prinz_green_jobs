# Scaling running

To run the measures of our sample of 1 million job adverts required some special scripts. These borrow heavily from `GreenMeasures`, but it made sense to structure each measure in a separate script and to save interim outputs.

## Occupation measures

Run the occupation measures for the large sample of job adverts.

```
python dap_prinz_green_jobs/pipeline/ojo_application/flows/ojo_occupation_measures.py --production

```

This was run locally and took about 1h 30m.

## Skills measures

```
python dap_prinz_green_jobs/pipeline/ojo_application/flows/ojo_skills_measures.py --production

```

This was run locally and took about 6h.

## Industry measures

```
python dap_prinz_green_jobs/pipeline/ojo_application/flows/ojo_industry_measures.py --production

```

This was run on EC2 and took about 11 days.

Installing faiss on the EC2 machine was hard, here is a log of what was done, although it's unclear which bit made it work:

```
conda install -c pytorch faiss-cpu=1.7.4 mkl=2021 blas=1.0=mkl
pip install faiss-cpu
```
