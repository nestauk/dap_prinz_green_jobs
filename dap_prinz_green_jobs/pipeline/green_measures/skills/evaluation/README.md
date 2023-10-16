# 🤔 Evaluation

## generate skills evaluation sample

To generate an evaluation sample for extracted and mapped skills and green skills, run the following:

```
python dap_prinz_green_jobs/pipeline/evaluation/skills_evaluation_sample.py
```

This directory contains a notebook that evaluates the overall **SkillMeasures** class. If you would like to learn more about the evaluation results of the green skill classifier, please refer to the Green skills classifier section of the [skills README](https://github.com/nestauk/dap_prinz_green_jobs/tree/dev/dap_prinz_green_jobs/pipeline/green_measures/skills#green-skills-classifier).

## 🖊️ Overall Evaluation

**514** skills from **500** job ads were labelled for:

- extracted skill quality (1-bad, 2-ok and 3-excellent)
- mapped green esco skill quality (0-extracted skill too poor to label, 1-bad, 2-ok and 3-excellent)
- mapped all esco skill quality (0-extracted skill too poor to label, 1-bad, 2-ok and 3-excellent)

Overall, in the labelled dataset of skills:

there were...

- **494** unique extracted skills
- **309** unique mapped all ESCO skills
- **182** unique green ESCO skills
- **1** skill labelled as 0 (skill entity too bad to score) for green esco matches
- **3** skills labelled as 0 (skill entity too bad to score) for all esco matches

The distribution of labels are as follows:

[DISTRIBUTION OF SKILLS GRAPH]

Example **excellent** matches to **green ESCO**:

| skill_label                                                                  | green_esco_skill                                     | green_esco_skill_id                  | is_good_skill_entity | is_good_green_esco |
| ---------------------------------------------------------------------------- | ---------------------------------------------------- | ------------------------------------ | -------------------- | ------------------ |
| deliver hygiene sustainability                                               | perform environmentally friendly cleaning activities | 4fb7f781-9362-426a-9eb6-65c360f83e31 | 3                    | 3                  |
| Environmental Assessment                                                     | conduct environmental site assessments               | f12f094b-492f-4e88-b36b-50b8e58b7500 | 3                    | 3                  |
| reduce energy consumption, advise on sustainability                          | encourage sustainable energy use                     | 1a6c7e0d-fc13-41d7-a5c0-8ca00606de89 | 3                    | 3                  |
| waste management                                                             | waste management                                     | 40f65a56-ccbe-4601-9f32-1cc6cdd24f28 | 3                    | 3                  |
| Embedding sustainability practices                                           | advocate sustainability                              | 469e19ed-a0bd-445a-ae2d-4ba9430e296b | 3                    | 3                  |
| Ensuring that potable water quality                                          | managing water quality testing                       | 1aae38c8-f75a-4009-8aa0-c5fa94b06e6b | 2                    | 3                  |
| Proposing and ensuring packaging standards are met                           | advocate sustainable packaging                       | 51399b1c-15d6-4e4c-8673-30c25a094fce | 3                    | 3                  |
| interest in environmental sustainability and the green agenda Flexibility to | environmental sustainability                         | b1b118c4-3291-484e-b64d-6d51fd5da8b3 | 2                    | 3                  |
| Experience in the waste sector                                               | waste management                                     | 40f65a56-ccbe-4601-9f32-1cc6cdd24f28 | 3                    | 3                  |
| quality of the physical environment and health and safety                    | coverage of health and safety standards              | f2f463e5-2382-49cb-8199-e7e043d868df | 1                    | 3                  |
| development of environmental sustainability criteria                         | environmental sustainability                         | b1b118c4-3291-484e-b64d-6d51fd5da8b3 | 3                    | 3                  |
| understanding of sustainability targets                                      | sustainability                                       | b1b118c4-3291-484e-b64d-6d51fd5da8b3 | 3                    | 3                  |
| Health and safety compliance                                                 | health and safety regulations                        | f2f463e5-2382-49cb-8199-e7e043d868df | 3                    | 3                  |

Example **ok** matches to **green ESCO**:

| skill_label                                                                                  | green_esco_skill                                                      | green_esco_skill_id                  | is_good_skill_entity | is_good_green_esco |
| -------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- | ------------------------------------ | -------------------- | ------------------ |
| Driving, co-ordinating and managing new innovations in Sustainability                        | promote sustainability                                                | 469e19ed-a0bd-445a-ae2d-4ba9430e296b | 3                    | 2                  |
| Applying MandG Real Estate's Sustainability requirements demonstrating appreciation of costs | promoting sustainability                                              | 469e19ed-a0bd-445a-ae2d-4ba9430e296b | 3                    | 2                  |
| to Politics, Public Policy or Sustainability                                                 | sustainability                                                        | b1b118c4-3291-484e-b64d-6d51fd5da8b3 | 1                    | 2                  |
| knowledge of the European Union's processes and sustainability policies                      | develop sustainable development policies                              | 507e2b21-1285-47e9-bf09-0db794df1bf0 | 3                    | 2                  |
| carrying out Sustainability Appraisals                                                       | advocate sustainability                                               | 469e19ed-a0bd-445a-ae2d-4ba9430e296b | 3                    | 2                  |
| passion for sustainability, collaboration                                                    | sustainability                                                        | b1b118c4-3291-484e-b64d-6d51fd5da8b3 | 3                    | 2                  |
| air quality modelling studies                                                                | air quality management                                                | 9f763ac3-1d6c-48d2-ab54-324b1b144f40 | 3                    | 2                  |
| supporting social and environmental sustainability outcomes                                  | offer suggestions on social responsibility and sustainability matters | b2f05068-c409-43ec-ba58-b5dfe991ca5e | 3                    | 2                  |
| managing planning                                                                            | management plans developing                                           | 0bca9840-478a-40fd-a6c3-c8cc923add98 | 3                    | 2                  |
| and Sustainability and Transformation Plans (STP’s)                                          | sustainability                                                        | b1b118c4-3291-484e-b64d-6d51fd5da8b3 | 2                    | 2                  |
| workspace design, architecture, art, furniture, materials and sustainability                 | promote sustainable interior designs                                  | 9263f0d8-7266-4782-b0ee-38f62656e97e | 2                    | 2                  |

Example **bad** matches to **green ESCO**:

| skill_label                                                          | green_esco_skill                                             | green_esco_skill_id                  | is_good_skill_entity | is_good_green_esco |
| -------------------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------ | -------------------- | ------------------ |
| attract the brightest minds in the energy sector                     | develop energy policy                                        | c4e9f0d0-bbdd-42d7-83a5-2c131f6d67c2 | 2                    | 1                  |
| effective project planning, financial and risk management            | prepare management plans                                     | 0bca9840-478a-40fd-a6c3-c8cc923add98 | 3                    | 1                  |
| maintaining the vital infrastructure and public spaces               | foster design of sustainable infrastructure                  | b251544f-7b92-46d5-8e14-97f59ce1c7dc | 3                    | 1                  |
| delivering sustainability reporting                                  | promote sustainability                                       | 469e19ed-a0bd-445a-ae2d-4ba9430e296b | 3                    | 1                  |
| build the infrastructure                                             | foster innovative infrastructure design                      | b251544f-7b92-46d5-8e14-97f59ce1c7dc | 2                    | 1                  |
| advanced metering infrastructure                                     | meter pollution                                              | 5aec5b83-df71-483e-90b9-21e56a0a3cb2 | 2                    | 1                  |
| experience, qualification or interest in the sustainability industry | advising on social responsibility and sustainability matters | b2f05068-c409-43ec-ba58-b5dfe991ca5e | 3                    | 1                  |
| projects they are looking for an Energy and Sustainability           | provide expertise on international energy projects           | c565e722-0e14-4f57-99ff-267dd9ea2510 | 1                    | 1                  |
| Maintaining safety                                                   | safety engineering                                           | cab07957-4f35-4bdd-9d6d-dc5a0cf42e13 | 1                    | 1                  |
| carrying out installations of energy saving products                 | carrying out energy management of facilities                 | 885808f7-f42b-406e-95f2-28efcd45ddda | 3                    | 1                  |
| thermal modelling and sustainability analysis                        | designing thermal equipment                                  | f2071535-4da0-4253-a548-920d170b80e0 | 3                    | 1                  |
| Prepare climate change-related documents                             | historical climate change studies                            | 32a76797-6235-4659-869f-c691ffd30463 | 3                    | 1                  |

### Qualitative observations

👍 The SkillMeasures appears to **do well** when:

1. It is matched to a **green ESCO** skill as opposed to any ESCO skill.
2. The entity has **more** context i.e. write sustainability reports vs. sustainability reports.

👎 It **does less well** when:

1. The extracted entity is poor (although this isn't always the case!)
2. The entity has **less** context i.e. sustainability reports vs. write sustainability reports.
