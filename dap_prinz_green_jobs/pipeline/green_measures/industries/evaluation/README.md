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

Example **ok** matches:

|       id | company_description                                                                                                                                                                                                                                                                 |   sic_code | sic_name                                                                                           |\n|---:|---------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------:|:---------------------------------------------------------------------------------------------------|\n|  0 | 47373757 | Finance Manager Your new company.  A global privately owned unique ethical entertainment business in Richmond are recruiting for a Finance Manager.                                                                                                                                 |      64921 | Credit granting by non-deposit taking finance houses and other specialist consumer credit grantors |\n|  1 | 45390261 | We are now one of the leading companies in the UK delivering  a range of energy efficiency and renewable measures and we are looking for people to join our incredible team.                                                                                                        |         96 | Other personal service activities                                                                  |\n|  2 | 45039536 | At Rendall and Rittner, we focus on delivering outstanding management to our clients and lessees.  This role will be based at a luxury residential development in Belgravia and compromises a collection of apartments, penthouses and townhouses, set around seven garden squares. |         47 | Retail trade, except of motor vehicles and motorcycles                                             |\n|  3 | 43429942 | Client Details A well regarded business with continued investment and growth are looking to add to their team.                                                                                                                                                                      |      64301 | Activities of investment trusts                                                                    |\n|  4 | 46104881 | Our prestigious client who is based in Bishop’s Stortford are wanting a highly creative and ambitious Salesforce Developer.                                                                                                                                                         |      58210 | Publishing of computer games                                                                       |

Example **good** matches:

|       id | company_description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |   sic_code | sic_name                                                  |\n|---:|---------:|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------:|:----------------------------------------------------------|\n|  0 | 41796496 | Management Accountant| Luxury Fashion Business | Rapid Growth The client  Harmonic are currently working exclusively with a rapidly growing luxury fashion label.  The CEO and Founder is an award-winning designer championing diversity and raising awareness of key political and environmental issues in her collections.                                                                                                                                                                                                                                                                                                                                                                                                                                     |      14190 | Manufacture of other wearing apparel and accessories      |\n|  1 | 44336516 | The Company Our client has over 15 years’ experience and expertise in property-related activity ranging from regeneration and modernisation to new build schemes.  Since 2001 they have built more than 3000 new homes and are now one of the North East's most successful housing developers.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |        412 | Construction of residential and non-residential buildings |\n|  2 | 46985053 | As a charity that cares for older people, you should be passionate about reducing loneliness and isolation and determined to make a difference for the 17,000 individuals we support across the country.  About us Methodist Homes (MHA) is the largest charity care provider in the U.  We have more than 75 years' experience of delivering care and support to over 18,500 older people.  As the largest charity care provider in the UK, we offer some of the highest quality care, accommodation and support services for older people throughout Britain.  Our mission is to inspire the best care and wellbeing at every stage of later life.                                                                                                              |        873 | Residential care activities for the elderly and disabled  |\n|  3 | 48472248 | The Company  Respected as a multi-national clothing retail giant, the business is dedicated to serving their customers and continuing to innovate and expand whilst striving for excellence in sustainability in the fashion  industry.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |      14190 | Manufacture of other wearing apparel and accessories      |\n|  4 | 43447234 | Head of Retail - Digital Transformation - Digital, Data, Innovation, Sustainability, Traceability£80K - £90K + Bonus + Excellent Benefits Our client is a global leader in the supply of professional services to the retail sector.  These services identify, capture and share information driving efficiencies and innovations within major retailers (grocery, apparel, gm retailers etc).  The role of Head  of Retail will to work across sector with businesses large and small to solve the problems they can't solve on their own helping them to realise the benefits of these standards within their organisations.  The ideal candidate will have a solid background with a major retailer or technology provider engaging with these  organisations. |      47990 | Other retail sale not in stores, stalls or markets        |

Example **bad** matches:

|       id | company_description                                                                                                                                                                                                                                                                                                                                    |   sic_code | sic_name                                                 |\n|---:|---------:|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------:|:---------------------------------------------------------|\n|  0 | 48245981 | Your new company.  This academy is a charity that harnesses the power to build a sustainable society and an inclusive economy that works for everyone.                                                                                                                                                                                                 |      85420 | Tertiary education                                       |\n|  1 | 45739409 | We have a brand new role for a Compliance Officer working for one of our North Somerset clients The company The company is an SME food supplier, that is part of a larger international group.                                                                                                                                                         |        462 | Wholesale of agricultural raw materials and live animals |\n|  2 | 47939191 | Our company expansion has evolved over time, making us the  leading global specialty company we are today.                                                                                                                                                                                                                                             |      64205 | Activities of financial services holding companies       |\n|  3 | 44087848 | Flexible working options A bit about us  At Menzies Distribution, we have been delivering across the UK since 1833.  Today, we are Logistic UK’s most innovative business of the year, with a strong focus on the future, sustainability, and innovation – we keep moving forwards.                                                                    |      46660 | Wholesale of other office machinery and equipment        |\n|  4 | 48197038 | Marketing Manager - Agency (In-House)Marketing Manager - The Company.  My client has built an impeccable reputation as a new-era sustainability solution for marketing teams who are looking to streamline and accelerate content production and distribution. Major Players are the UK's leading digital, marketing, creative and tech talent agency. |      68320 | Management of real estate on a fee or contract basis     |

### Qualitative observations

👍 The SICMapper appears to **do well** when:

1. When the company description is explicit about the industry (i.e. "We are a manufacturing company")
2. When the company description contains multiple sentences that are explicit. (i.e. "We are a manufacturing company. We manufacture cars, bikes and planes.")

👎 It **does less well** when:

1. The company name (although not labelled a "recruiter consultancy") is actually a recruiter consultancy. (i.e. the Guardian)
2. When a term has multiple meanings (i.e. "Juice is recruiting for a manufacturing company" was mapped to a food related SIC code)
3. When a company description is vague (i.e. "We are a company committed to the future.")
