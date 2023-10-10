## Evaluation

This directory contains the evaluation of the company description classifier and the SICMapper. It also contains a notebook to generate evaluation graphs on a labelled dataset of X job ads.

### Company Description Classifier Evaluation

To learn more about the evaluation results of the company description classifier, refer to [📠 20230825 model metrics](https://github.com/nestauk/ojd_daps_language_models/tree/dev/ojd_daps_language_models/pipeline/train_model/company_descriptions#-20230825-model-metrics) in the `OJD DAPs language models repo`.

### Null and thresholding analysis

### SICMapper Evaluation

When we labelled company descriptions, we also asked the LLM to assign it to a SIC code. We manually verified whether the match was appropriate or not.

We have labelled **287** job ads with SIC codes. On that evaluation set, company descriptions are extracted **93.2%** of the time.

We then further verified **85** matches and found…

- 73% of mapped SIC codes are good or ok

- 27% of mapped SIC codes are bad

- 49% of matches were at least the same quality or better than the LLM

- 40% of matches were worse than the LLM

- In 11% of cases, both the LLM and the current approach were bad
