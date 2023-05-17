"""
Extract green measures pipeline
--------------
A pipeline that extracts green measures on a OJO sample
    as defined in dap_prinz_green_jobs/getters/data_getters/ojo.py

It also saves the extracted green measures to s3.

python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_measures_flow.py run
"""
from dap_prinz_green_jobs.pipeline.green_measures.green_measures import GreenMeasures
from dap_prinz_green_jobs.getters.ojo_getters import (
    get_ojo_skills_sample,
    get_ojo_sample,
    get_ojo_job_title_sample,
)

from dap_prinz_green_jobs import logger

from toolz import partition_all
from metaflow import FlowSpec, step, Parameter

# instantiate GreenMeasures class here
gm = GreenMeasures()


class ExtractGreenMeasuresFlow(FlowSpec):
    production = Parameter("production", help="Run in production?", default=False)
    batch_size = Parameter("batch_size", help="Batch size", default=5000)

    @step
    def start(self):
        """
        Start step
        """
        self.next(self.load_ojo_sample)

    @step
    def load_ojo_sample(self):
        """
        loads ojo skills sample from s3 and reformat it to be compliant with GreenMeasures
        """
        import dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils as sm

        # also load extracted skills so as not to duplicate skills extraction
        logger.info("loading and reformattting extracted skills from ojo sample...")
        ojo_skills_raw = get_ojo_skills_sample()

        if not self.production:
            ojo_skills_raw = ojo_skills_raw[:1]

        # reformat it to be the output of es.get_skills() for GreenMeasures
        self.ojo_skills = (
            ojo_skills_raw.groupby("id")
            .skill_label.apply(list)
            .reset_index()
            .assign(skills_formatted=lambda x: x.skill_label.apply(sm.format_skills))
            .skills_formatted.tolist()
        )

        # chunk job skills
        logger.info("chunking already extracted job skills...")

        if self.production:
            self.job_skill_chunks = list(
                partition_all(self.batch_size, self.ojo_skills)
            )
        else:
            self.job_skill_chunks = list(partition_all(1, self.ojo_skills))

        self.next(self.extract_skill_measures, foreach="job_skill_chunks")

    @step
    def extract_skill_measures(self):
        """
        extract skill measures for each job skill chunk
        """
        logger.info("mapping green skills for each data chunk...")

        # you're just mapping skills, not extracting
        self.green_skill_chunks = gm.get_skill_measures(skill_list=self.input)

        self.next(self.join_skill_measures)

    @step
    def join_skill_measures(self, inputs):
        """Join skill measure outputs"""
        from itertools import chain

        logger.info("joining green skill outputs...")
        self.green_skill_outputs = list(chain(*[i.green_skill_chunks for i in inputs]))

        self.next(self.extract_other_green_measures)

    @step
    def extract_other_green_measures(self):
        """
        extract industry and occupation measures for ojo sample
        """
        import pandas as pd

        # load current ojo sample
        logger.info("loading and reformattting ojo sample...")
        ojo_sample_raw, ojo_job_title_raw = get_ojo_sample(), get_ojo_job_title_sample()
        ojo_sample_raw_title = pd.merge(ojo_sample_raw, ojo_job_title_raw, on="id")

        if not self.production:
            ojo_sample_raw_title = ojo_sample_raw_title[:1]
        # reformat it to be a list of dictionaries for GreenMeasures
        ojo_sample = list(
            (
                ojo_sample_raw_title[["job_title_raw_x", "company_raw", "description"]]
                .rename(
                    columns={
                        "job_title_raw_x": gm.job_title_name,
                        "company_name": gm.company_name,
                        "description": gm.job_text_name,
                    }
                )
                .T.to_dict()
                .values()
            )
        )
        logger.info("extracting green industries and occupations for ojo sample...")

        green_industry_outputs = gm.get_industry_measures(job_advert=ojo_sample)
        green_occupation_outputs = gm.get_occupation_measures(job_advert=ojo_sample)

        # create dictionary with all green measures
        self.green_outputs = {
            "SKILL MEASURES": self.green_skill_outputs,
            "INDUSTRY MEASURES": green_industry_outputs,
            "OCCUPATION MEASURES": green_occupation_outputs,
        }

        self.next(self.end)

    @step
    def end(self):
        """
        save merged outputs to s3
        """
        from dap_prinz_green_jobs.getters.data_getters import save_to_s3
        from dap_prinz_green_jobs import BUCKET_NAME

        logger.info("saving green measures to s3...")
        save_to_s3(
            BUCKET_NAME,
            self.green_outputs,
            f"outputs/data/ojo_application/ojo_sample_green_measures_production_{self.production}.json",
        )


if __name__ == "__main__":
    ExtractGreenMeasuresFlow()
