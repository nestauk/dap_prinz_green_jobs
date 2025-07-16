## Green jobs analysis for Wales

We did some bespoke analysis for green jobs in Wales, also incorporating some datasets from the [job quality](https://github.com/nestauk/dap_job_quality) piece of work.

This involved some data wrangling to get the neccessary insights as well as getting datasets into a format needed for plotting in Flourish. All of this was done in various notebooks. Note that these notebooks need some refactoring and cleaning up, and all data saved is saved locally to this folder.

The Flourish story about the data created here can be found [here](https://public.flourish.studio/story/2915045/).

### `occupation_labels_data_step_1.ipynb`

- Create a dataset of the numbers of job adverts in Wales and England for each occupation as well as other measures.
- Find the 'greenest' occupations for Wales, a subjective task informed by our green measures.
- This is to label which occupations should be included in job quality analysis.

### `merge_job_quality_step_2.ipynb`

- Read in the job quality measures calculated for a subset of job adverts.
- Combine this with green measures data.

### `analyse_job_quality_step_3.ipynb`

- Get aggregated job information for the Welsh job adverts, both for all of them and for just those which have job quality measures too.

### `combine_mid_salaries_4.ipynb`

- Create salary bands for the 'greenest' and least green occupations for Wales.

### `reformat_for_flourish.ipynb`

- Read in some of the datasets created from the notebooks above and format them for Flourish if needed.

### `Welsh_analysis.ipynb`

- An older notebook to create data for the analysis of Welsh job adverts.
