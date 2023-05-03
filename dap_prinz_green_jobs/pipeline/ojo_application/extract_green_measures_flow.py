"""
Extract green measures pipeline
--------------
A pipeline that extracts green measures on a OJO sample
    as defined in dap_prinz_green_jobs/getters/data_getters/ojo.py

It also saves the extracted green measures to s3.

python dap_prinz_green_jobs/pipeline/ojo_application/extract_green_measures_flow.py run
"""
from dap_prinz_green_jobs.pipeline.green_measures.green_measures import GreenMeasures
from dap_prinz_green_jobs import logger

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
        loads ojo sample from s3 and reformat it to be compliant with GreenMeasures
        """
        from dap_prinz_green_jobs.getters.ojo import (
            get_ojo_sample,
            get_ojo_skills_sample,
        )
        import dap_prinz_green_jobs.pipeline.green_measures.skills.skill_measures_utils as sm
        from toolz import partition_all

        # load current ojo sample

        logger.info("loading and reformattting ojo sample...")
        ojo_sample_raw = get_ojo_sample()

        ojo_sample_raw if self.production else ojo_sample_raw[:1]

        # reformat it to be a list of dictionaries for GreenMeasures
        self.ojo_sample = list(
            (
                ojo_sample_raw
                # currently to deal with no company name
                .assign(company_name=lambda x: "Nesta")[
                    ["job_title_raw", "company_name", "description"]
                ]
                .rename(
                    columns={
                        "job_title_raw": gm.job_title_name,
                        "company_name": gm.company_name,
                        "description": gm.job_text_name,
                    }
                )
                .T.to_dict()
                .values()
            )
        )

        # also load extracted skills so as not to duplicate skills extraction
        logger.info("loading and reformattting extracted skills from ojo sample...")
        ojo_skills_raw = get_ojo_skills_sample()

        ojo_skills_raw if self.production else ojo_skills_raw[:1]

        # reformat it to be the output of es.get_skills() for GreenMeasures
        self.ojo_skills = (
            ojo_skills_raw.groupby("id")
            .skill_label.apply(list)
            .reset_index()
            .assign(skills_formatted=lambda x: x.skill_label.apply(sm.format_skills))
            .skills_formatted.tolist()
        )

        # chunk both datasets
        logger.info("chunking job adverts and skills...")
        self.job_advert_chunks, self.job_skill_chunks = list(
            partition_all(self.batch_size, self.ojo_sample)
        ), list(partition_all(self.batch_size, self.ojo_skills))

        self.next(self.extract_green_measures, foreach="job_advert_chunks")

    @step
    def extract_green_measures(self):
        """
        extract green measures for each job advert chunk
        """
        logger.info("extracting green measures for each data chunk...")

        # make sure you're also passing skill chunks
        self.green_output_chunks = gm.extract_green_measures(
            job_advert=self.input, skill_list=self.input
        )

        self.next(self.join)

    @step
    def join(self, input):
        """Join green measure outputs and save to s3"""
        from collections import ChainMap
        from dap_prinz_green_jobs.getters.data_getters import save_to_s3
        from dap_prinz_green_jobs import BUCKET_NAME

        logger.info("joining green measure outputs...")
        green_outputs = dict(ChainMap(*[i.green_output_chunks for i in input]))

        save_to_s3(
            BUCKET_NAME,
            green_outputs,
            "outputs/data/ojo_application/ojo_sample_green_measures.json",
        )

    @step
    def end(self):
        """
        End step
        """
        pass


if __name__ == "__main__":
    ExtractGreenMeasuresFlow()
