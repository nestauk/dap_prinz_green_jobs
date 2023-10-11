# 🤔 Evaluation

This directory contains a notebook that evaluates the **overall SICMapper**. The notebook is split into the following sections:

**Null analysis:** On the full dataset of extracted industries measures, what proportion are null? This includes null analysis of SIC codes in addition to industries measures.

**Threshold analysis:** On the full dataset of extracted industries measures, what is the optimal threshold for the two SIC mapping approaches? This includes a thresholding analysis of the `closest SIC` and `majority SIC` approaches to better understand completeness.

**Labelled Evaluation:** This section is split into two subsections:

- one that explores a labelled dataset of job ads to explore the effect of different thresholds on accuracy.
- one that explores a different, labelled dataset of job ads (incl. SIC codes mapped via companies house) to explore overall accuracy.

If you would like to learn more about the evaluation results of the **company description classifier**, refer to [📠 20230825 model metrics](https://github.com/nestauk/ojd_daps_language_models/tree/dev/ojd_daps_language_models/pipeline/train_model/company_descriptions#-20230825-model-metrics) in the OJD DAPs language models repo.

## 🌊 Thresholding analysis

The base thresholds for the two SIC mapping approaches (as defined in `IndustryMeasures`) are as follows:

1. `closest SIC`: **0.5**
2. `majority SIC`: **0.3**

To determine the optimal threshold for each approach, we ran a thresholding analysis on a labelled dataset of **111** job ads (`random_state=42`). We binarily label 'ok' or 'good' match quality matches as 1 and 'bad' match quality matches as 0.

The results are as follows:

```
Number of job ads in threshold evaluation set: 111
Number of job ads with extracted SIC: 92
% of job ads WITH extracted SIC: 0.8288288288288288
% of job ads WITHOUT extracted SIC: 0.17117117117117117
```

The results are as follows for the `closest SIC` approach:

| Threshold | Accuracy | Number of Job Ads | % of Job Ads |
| --------- | -------- | ----------------- | ------------ |
| 0.5       | 0.81     | 57                | 0.81         |
| 0.55      | 0.875    | 32                | 0.45         |
| 0.6       | 0.94     | 17                | 0.24         |
| 0.65      | 0.67     | 3                 | 0.04         |

The results are as follows for the `majority SIC` approach:

| Threshold | Accuracy | Number of Job Ads | % of Job Ads |
| --------- | -------- | ----------------- | ------------ |
| 0.3       | 0.772    | 22                | 0.31         |
| 0.35      | 0.7      | 10                | 0.14         |
| 0.4       | 1        | 2                 | 0.03         |

## 🖊️ Overall Evaluation

Based on the thresholding analysis, we decided to use the following thresholds for the two SIC mapping approaches:

1. `closest SIC`: **0.5**
2. `majority SIC`: **0.3**

We then ran the SICMapper on a further random sample of **500** job ads (`random_state = 62`) and labelled **266** job ads. The results are as follows:

<p align="center">
  <img src="https://github.com/nestauk/dap_prinz_green_jobs/assets/46863334/389a69e1-3721-41cc-85f8-3bfd70ca1e4e" />
</p>

Example **good** matches:

| Company Description                                                                                                                                                                                                                                                                                                                                         | SIC Code | SIC Name                                                   |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | ---------------------------------------------------------- |
| Would you like to work for a company who prides themselves on setting their purpose to empower everyone to make the most of our energy and resources, bridging progress and sustainability for all.                                                                                                                                                         | 351      | Electric power generation, transmission and distribution   |
| Our client is a dynamic and hugely ambitious start-up focused on the financing, construction and operation of renewable infrastructure across the UK.                                                                                                                                                                                                       | 412      | Construction of residential and non-residential buildings  |
| This brilliant, hard-working, small agency helps their clients to develop and evaluate advertising and communications, primarily aimed at encouraging socially desirable behaviour. Many of their clients are big-hitting government departments or advertising agencies but they occasionally turn their hand to commercial research to keep things fresh. | 73       | Advertising and market research                            |
| On offer is the opportunity to work within a company that Design, Install and Maintain Rainwater harvesting and Greywater Recycling Systems. Their systems promote sustainability and reduce carbon footprint.                                                                                                                                              | 39       | Remediation activities and other waste management services |
| This fantastic school are looking for a Maths Teacher to join their team immediately to teach across the school, including A level Maths. As a school they aim to .                                                                                                                                                                                         | 85310    | General secondary education                                |

Example **ok** matches:

| Company Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  | SIC Code | SIC Name                                                                                                                    | Why                                         |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------- | --------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| 00 Our client has been delivering across the UK since 1833, They can reach areas that other cannot. Today, they are Logistic UK’s most innovative business of the year, with a strong focus on the future, sustainability, and innovation – They keep moving forwards. Our clients team are responsible for delivering world-class service and time critical parcels to their customers located throughout Scotland. And, let’s not forget, you’ll be joining one of the UK’s largest and most recognised distribution brands, where career, personal development and going the ‘extra mile’ is at the heart of their company ethos. | 512      | Freight air transport and space transport                                                                                   | Too specific but not incorrect              |
| Our client, a well-established asset management firm specialising in global and regional equity and multi-asset, is looking for a product specialist to join their product and strategy team.                                                                                                                                                                                                                                                                                                                                                                                                                                        | 74909    | Other professional, scientific and technical activities (not including environmental consultancy or quantity surveying) nec | Too vague but not incorrect                 |
| Agricultural Manager Belfast Days £40,000 to £50,000 We are looking for a motivated Agricultural manager to join a market leading manufacturer in their respective field.                                                                                                                                                                                                                                                                                                                                                                                                                                                            | 1        | Crop and animal production, hunting and related service activities                                                          | Too specfic but related to agriculture      |
| Client Details A well regarded business with continued investment and growth are looking to add to their team.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | 64301    | Activities of investment trusts                                                                                             | Company desciption is vague. As is SIC code |
| 00 per hour, DOE Energy and Environmental Engineer required to join a forward-thinking Manufacturing Engineering department based in Solihull on an ongoing contract basis.                                                                                                                                                                                                                                                                                                                                                                                                                                                          | 27110    | Manufacture of electric motors, generators and transformers                                                                 | Specific to a type of manufacturing         |

Example **bad** matches:

| Company Description                                                                                                                                                                                                                                                                                                                                                                                  | SIC Code | SIC Name                                                                                                             | Why                                                |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| Looking for the excitement of working for a leading London PR agency to be mentored by the top PR professionals in the industry.                                                                                                                                                                                                                                                                     | 78109    | Activities of employment placement agencies (other than motion picture, television and other theatrical casting) nec | Unrelated to PR                                    |
| Our clients, a Design and Property Consultancy are looking for an experienced Principal Electrical Engineer to join their growing team in Matlock. This is working for an innovative and growing company, that really looks after its team. They deal with multi million-pound projects within the public sector and cover areas in Derbyshire as well as projects within the South East of England. | 27110    | Manufacture of electric motors, generators and transformers                                                          | Company description does not mention manufacturing |
| Our company expansion has evolved over time, making us the leading global specialty company we are today.                                                                                                                                                                                                                                                                                            | 64205    | Activities of financial services holding companies                                                                   | Company description too vague                      |
| Eye Care A bit about our client Our client has been delivering across the UK since 1833. Today, they are Logistic UK’s most innovative business of the year, with a strong focus on the future, sustainability, and innovation – They keep moving forwards.                                                                                                                                          | 47782    | Retail sale by opticians                                                                                             | "Eye Care" should not be in company description    |
| We have an opportunity to join an exclusive Swedish beauty and wellness company as a Beauty Brand Partner.                                                                                                                                                                                                                                                                                           | 93190    | Other sports activities                                                                                              | No mention of sports                               |

### Qualitative observations

👍 The SICMapper appears to **do well** when:

1. When the company description is explicit about the industry (i.e. "We are a manufacturing company")
2. When the company description contains multiple sentences that are explicit. (i.e. "We are a manufacturing company. We manufacture cars, bikes and planes.")

👎 It **does less well** when:

1. The company name (although not labelled a "recruiter consultancy") is actually a recruiter consultancy. (i.e. the Guardian)
2. When a term has multiple meanings (i.e. "Juice is recruiting for a manufacturing company" was mapped to a food related SIC code)
3. When a company description is vague (i.e. "We are a company committed to the future.")
