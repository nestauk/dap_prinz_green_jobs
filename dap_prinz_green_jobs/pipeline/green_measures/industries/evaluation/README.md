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

To determine the optimal threshold for each approach, we ran a thresholding analysis on a labelled dataset of **X** job ads (`random_state=42`). We binarily label 'ok' or 'good' match quality matches as 1 and 'bad' match quality matches as 0.

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
  <img src="[http://some_place.com/image.png](https://github.com/nestauk/dap_prinz_green_jobs/assets/46863334/389a69e1-3721-41cc-85f8-3bfd70ca1e4e)" />
</p>

### Qualitative observations

👍 The SICMapper appears to **do well** when:

1. When the company description is explicit about the industry (i.e. "We are a manufacturing company")
2. When the company description contains multiple sentences that are explicit. (i.e. "We are a manufacturing company. We manufacture cars, bikes and planes.")

👎 It **does less well** when:

1. The company name (although not labelled a "recruiter consultancy") is actually a recruiter consultancy. (i.e. the Guardian)
2. When a term has multiple meanings (i.e. "Juice is recruiting for a manufacturing company" was mapped to a food related SIC code)
3. When a company description is vague (i.e. "We are a company committed to the future.")
