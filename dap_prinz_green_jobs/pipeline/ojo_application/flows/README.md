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

This was run on EC2 and took around 78 hours.

## Industry measures

```
python dap_prinz_green_jobs/pipeline/ojo_application/flows/ojo_industry_measures.py --production

```

This was run on EC2 and took about 27 hours.

Installing faiss on the EC2 machine was hard, here is a log of what was done, although it's unclear which bit made it work:

```
conda install -c pytorch faiss-cpu=1.7.4 mkl=2021 blas=1.0=mkl
pip install faiss-cpu
```

# Updating the data

## Industry measures

To get the industry measures for just the newest set of job adverts, and then to merge them with pre-calculated industry measures for older job adverts, update the file directories at the top of these scripts as needed (`green_ind_existing_data_dir` and `new_ojo_descriptions_dir`) and run:

```
python dap_prinz_green_jobs/pipeline/ojo_application/flows/ojo_industry_measures_update.py --production

```

On EC2 this took 45 hours.

This produces two files of interest in the `s3://prinz-green-jobs/outputs/data/ojo_application/extracted_green_measures/[DATE_RUN]/` folder:

1. `ojo_newest_industry_green_measures_production_True.csv`: the extracted green industry measures for the new job adverts, for `20241115` this was 1,313,447 job adverts.
2. `ojo_all_industry_green_measures_production_True.csv`: a merged file of industry measures for all the new and old job adverts, for `20241115` this was 5,967,229 job adverts.
