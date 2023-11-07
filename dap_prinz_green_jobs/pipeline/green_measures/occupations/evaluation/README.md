# 🤔 Evaluation

This directory contains the evaluation of the **overall SOCMapper**.

To get the evaluation sample we looked at the most common job titles given in all of the OJO dataset, and a random sample of job titles. These datasets can be created by running:

```
python dap_prinz_green_jobs/pipeline/green_measures/occupations/evaluation/socmapper_evaluation_sample.py
```

which will create `soc_evaluation_sample.csv` and `soc_evaluation_random_sample.csv`.

These datasets were manually labelled with how well we thought the job title was matched to a SOC code. We chose 3 categories - excellent, good or poor.

## 🖊️ Overall Evaluation

This analysis can be run from the notebook `socmapper_evaluation.ipynb`.

### From a **random sample** of 200 job titles:

- 59.6% had a SOC 6-digit code matched
- 5% were only able to have a SOC 4-digit code matched
- 35.5% had no SOC matched

Using 118 job titles of the random sample with SOC 6-digit codes found:

- 66% had excellent quality SOC matches
- 23% had good quality SOC matches
- 11% had poor quality SOC matches

From the 5% (10 job titles) of the random sample with SOC 4-digit codes found:

- 80% had excellent quality SOC matches
- 10% had good quality SOC matches
- 10% had poor quality SOC matches

### We also labelled 300 of the **most commonly occuring** job titles in our dataset with quality measures.

- 89% had a SOC 6-digit code matched
- 4% were only able to have a SOC 4-digit code matched
- 7% had no SOC matched

Using 255 job titles of the most commonly occuring job titles with SOC 6-digit codes found:

- 82% had excellent quality SOC matches
- 10% had good quality SOC matches
- 8% had poor quality SOC matches

From the 20 job titles of the most commonly occuring job titles with SOC 4-digit codes found:

- 95% had excellent quality SOC matches
- 5% had good quality SOC matches

We note that the results from the most commonly occuring job titles are probably better since the job title tends to be more clean and standardised.

### Examples of **excellent** matches:

| ojo_job_title                             | num_job_ads | prop_job_ads | soc_2020_6_name                          | occ_matched              | match_prob |
| ----------------------------------------- | ----------- | ------------ | ---------------------------------------- | ------------------------ | ---------- |
| Care Assistant - Bank - Care Home         | 22444       | 0.0031       | Domiciliary care workers                 | home care assistant      | 0.78       |
| Pastry Demi Chef de Partie                | 1           | 0.0000       | Chefs                                    | chef de partie           | 0.79       |
| Forklift Driver                           | 2922        | 0.0004       | Fork-lift truck drivers                  | fork lift truck driver   | 0.88       |
| Finance ManagerRB                         | 1           | 0.0000       | Company secretaries and finance managers | finance manager          | 0.85       |
| Service Engineer Carpentry and Decorating | 1           | 0.0000       | Painters and decorators                  | decorating contractor    | 0.72       |
| Senior Software Engineer                  | 2681        | 0.0004       | Software developers                      | senior software engineer | 1.00       |
| Change Business Analyst - FMCG experience | 1           | 0.0000       | Business analysts                        | business change analyst  | 0.69       |
| Private Client Solicitor                  | 5338        | 0.0007       | Solicitors and lawyers n.e.c.            | solicitor                | 0.75       |
| Internal Sales Executive                  | 2281        | 0.0003       | Business sales executives                | sales executive          | 0.85       |
| HR Advisor                                | 10386       | 0.0014       | Human resources advisors                 | human resources adviser  | 0.85       |

### Examples of **good** matches:

| ojo_job_title                            | num_job_ads | prop_job_ads | soc_2020_6_name                                       | occ_matched                     | match_prob |
| ---------------------------------------- | ----------- | ------------ | ----------------------------------------------------- | ------------------------------- | ---------- |
| Domestic Assistant                       | 2934        | 0.0004       | Commercial cleaners                                   | domestic assistant              | 1          |
| Holiday Club Admin Manager               | 1           | 0.0000       | Hotel and accommodation managers and proprietors      | holiday centre manager          | 0.79       |
| Training and Support Manager             | 1           | 0.0000       | Education managers                                    | learning support manager        | 0.80       |
| Digital Marketing Executive              | 4554        | 0.0006       | Marketing consultants                                 | digital marketing executive     | 1          |
| Operations Manager -Commercial Insurance | 1           | 0.0000       | Financial managers and directors n.e.c.               | insurance company manager       | 0.79       |
| Field Service Engineer                   | 7272        | 0.0010       | Telecoms and related network installers and repairers | home service field engineer     | 0.87       |
| Tutor                                    | 3370        | 0.0005       | Higher education teaching professionals n.e.c.        | course tutor                    | 0.92       |
| Assistant Manager - Truro                | 2           | 0.0000       | Other administrative occupations n.e.c.               | manager's assistant             | 0.78       |
| Marketing Executive                      | 8363        | 0.0012       | Marketing consultants                                 | marketing executive             | 1          |
| Chartered Financial Advisor - Berkshire  | 2           | 0.0000       | Financial accountants                                 | chartered management accountant | 0.70       |

### Examples of **bad** matches:

| ojo_job_title                                                     | num_job_ads | prop_job_ads | soc_2020_6_name                                         | occ_matched                       | match_prob |
| ----------------------------------------------------------------- | ----------- | ------------ | ------------------------------------------------------- | --------------------------------- | ---------- |
| Academic Mentor                                                   | 2847        | 0.000        | Learning and behaviour mentors                          | learning mentor                   | 0.85       |
| Electronics Assembly Technician - Oxford - &#163;30,000 per annum | 2           | 0.000        | Metal working production and maintenance fitters n.e.c. | assembly engineer                 | 0.67       |
| Senior Administrator                                              | 2315        | 0.000        | Registrars                                              | senior registration administrator | 0.84       |
| Census officer                                                    | 3547        | 0.000        | Office managers                                         | census district manager           | 0.80       |
| Operative                                                         | 2201        | 0.000        | Textile process operatives n.e.c.                       | general operative                 | 0.77       |
| Business Case Manager Business                                    | 1           | 0.000        | National government administrative occupations n.e.c.   | case manager                      | 0.76       |
| Production Operative                                              | 16113       | 0.002        | Printing machine assistants                             | finishing operative               | 0.76       |
| Night Care Assistant                                              | 11302       | 0.002        | Shelf fillers                                           | night assistant                   | 0.86       |
| Supply Teacher                                                    | 7316        | 0.001        | Inventory and stock controllers                         | supplies superintendent           | 0.72       |
| Carpenter - Timber Frame                                          | 1           | 0.000        | Agricultural and fishing trades n.e.c.                  | timber contractor                 | 0.72       |

### Observations

A random sample of the job titles that don't match to a SOC revealed that extra cleaning may help them match to SOC codes. Some of the job titles that didn't match include:

['Disability Assessor Homebase / Front Office', 'Clinical Fellow ST3 Stroke medicine', 'IT Engineer - &#163;25-&#163;30k - Normanton / Hybrid', 'Entry Level Graduate Scheme', 'Operatives Needed', 'Waiting Staff 1', 'Staff Nurse, General Surgery - Band 5', 'PHP Developer Laravel Vue.js', 'Bike Courier parttime Liverpool', 'E&amp;I Technician Days', '1-1 Tutor Required - Wakefield', 'Flexcube Analyst permanent', 'Infection Prevention Control Nurse - Band 8a', 'Blinds and Curtains Installer', 'Senior Community Host - Woking', 'Data Architect, Microsoft Stack, Remote', 'Factory Cleaning Operative - &#163;1000 sign on bonus!', 'Retail Customer Service CSM 30hrs - Multi-site', 'Retail Customer Service CSA 30hrs', 'Driver weekend Liverpool']

## Future work

It'd be good to compare our SOCmapper performance to other mappers out there, for example [this python package that maps to 3-digit SOC](https://github.com/aeturrell/occupationcoder) or [the online tool from Cascot](https://cascotweb.warwick.ac.uk/#/classification/soc2020).
