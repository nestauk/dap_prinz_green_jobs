# 🤔 Evaluation

This directory contains the evaluation of the SICMapper. It also contains a notebook to generate evaluation graphs on a labelled dataset of X job ads.

## 📠 Company Description Classifier Evaluation

To learn more about the evaluation results of the company description classifier, refer to [📠 20230825 model metrics](https://github.com/nestauk/ojd_daps_language_models/tree/dev/ojd_daps_language_models/pipeline/train_model/company_descriptions#-20230825-model-metrics) in the `OJD DAPs language models repo`.

## 🌊 Thresholding analysis

The base thresholds for the two SIC mapping approaches (as defined in `IndustryMeasures`)are as follows:

1. `closest SIC`: **0.5**
2. `majority SIC`: **0.3**

To determine the optimal threshold for each approach, we ran a thresholding analysis on a labelled dataset of **111** job ads. We binarily label 'ok' or 'good' match quality matches as 1 and 'bad' match quality matches as 0.  

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

We also generated graphs to visualise the results:

![threshold_matching](https://github.com/nestauk/dap_prinz_green_jobs/assets/46863334/141028c4-3c99-4c3b-b36a-9ecc2006dfd7)

Ultimately, the thresholds as defined in `dap_prinz_green_jobs/config/base.yaml` appear reasonable. 
