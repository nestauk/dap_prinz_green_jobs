"""
Extract green skills pipeline
---------

A pipeline that takes a sample of job adverts and extacts and maps
skills onto a custom green skills taxonomy.

(with default parameters):

python dap_prinz_green_jobs/pipeline/ojo_application/green_skills/extract_green_skills_flow.py run --extract_skills_library_path "PATH/TO/YOUR/LIBRARY-PACKAGE"
"""
from metaflow import FlowSpec, S3, step, Parameter
from dap_prinz_green_jobs.getters.data_getters import (
    get_s3_resource,
    load_s3_data,
    save_to_s3,
    save_json_dict,
)
from dap_prinz_green_jobs import BUCKET_NAME, PROJECT_DIR, get_yaml_config, logger

from ojd_daps_skills.pipeline.extract_skills.extract_skills import (
    ExtractSkills,
)  # import the module


s3 = get_s3_resource()


class GreenSkillsFlow(FlowSpec):
    """This flow instantiates the Skills Extractor class with a
    custom config file for a formatted green skills list
    and extracts skills from a sample of job adverts
    using the custom config file.
    """

    production = Parameter("production", help="Run in production?", default=False)
    config_name = Parameter(
        "config_name",
        help="Name of the config file to use",
        default="extract_green_skills_esco",
    )
    job_advert_sample_path = Parameter(
        "job_advert_sample_path",
        help="s3 path to the job advert sample",
        default="outputs/data/job_ads/job_ads_sample.csv",
    )
    extract_skills_library_path = Parameter(
        "extract_skills_library_path",
        help="library path to add custom files to",
        default="/Users/india.kerlenesta/opt/anaconda3/envs/dap_prinz_green_jobs/lib/python3.8/site-packages/ojd_daps_skills",
    )
    chunk_size = Parameter(
        "chunk_size", help="size of chunks to split job adverts into", default=5000
    )

    @step
    def start(self):
        """
        Starts the flow.
        """
        self.next(self.add_custom_config)

    @step
    def add_custom_config(self):
        """
        Instantiates the ExtractSkills class with a custom config file.
        """
        import shutil
        import os

        logger.info(
            "adding custom config and data files to relevent ojd-daps-skills library folder..."
        )
        custom_config = get_yaml_config(
            PROJECT_DIR / f"dap_prinz_green_jobs/config/{self.config_name}.yaml"
        )

        # copy the custom config file to the extract skills location
        shutil.copy(
            PROJECT_DIR / f"dap_prinz_green_jobs/config/{self.config_name}.yaml",
            f"{self.extract_skills_library_path}/config/{self.config_name}.yaml",
        )

        # move custom formatted esco green skills to extract skills location
        custom_green_skills_path = os.path.join(
            "outputs/data/green_skill_lists",
            custom_config["taxonomy_path"].split("/")[-1],
        )
        formatted_esco_green_skills = load_s3_data(
            s3, BUCKET_NAME, custom_green_skills_path
        )
        formatted_esco_green_skills.to_csv(
            f"{self.extract_skills_library_path}_data/{custom_config['taxonomy_path']}"
        )

        esco_green_skill_embeddings_path = os.path.join(
            "outputs/data/green_skill_lists",
            custom_config["taxonomy_embedding_file_name"].split("/")[-1],
        )
        esco_green_skill_embeddings = load_s3_data(
            s3, BUCKET_NAME, esco_green_skill_embeddings_path
        )
        save_json_dict(
            esco_green_skill_embeddings,
            f"{self.extract_skills_library_path}_data/{custom_config['taxonomy_embedding_file_name']}",
        )

        # load ExtractSkills class with custom config
        logger.info("instantiating Extract Skills class with custom config...")
        self.skills_extractor = ExtractSkills(self.config_name)

        self.skills_extractor.load()

        self.next(self.load_job_adverts_sample)

    @step
    def load_job_adverts_sample(self):
        """
        Loads the job adverts sample from s3.
        """
        import pandas as pd
        from toolz import partition_all
        from dap_prinz_green_jobs.getters.ojo import get_ojo_sample

        # this step to be replaced with i.e. load_ojo_sample() if we move this to getters
        logger.info("loading job advert sample from s3...")

        ojo_data = get_ojo_sample()
        job_adverts = ojo_data.text.to_list()

        job_adverts if self.production else job_adverts[: self.chunk_size]

        logger.info("chunking job adverts...")
        self.job_ad_chunks = list(partition_all(self.chunk_size, job_adverts))

        self.next(self.extract_green_skills, foreach="job_ad_chunks")

    @step
    def extract_green_skills(self):
        """
        Extracts green skills from job adverts.
        """
        from datetime import datetime as date

        import itertools

        logger.info(f"extracting skills using {self.config_name} config...")

        self.green_skills = self.skills_extractor.extract_skills(self.input)

        self.next(self.join)

    @step
    def join(self, inputs):
        """Join extracted skills and save to s3."""
        import itertools

        # join forward citation results
        logger.info("joining extracted skills...")
        all_green_skills = [i.green_skills for i in inputs]
        self.flat_all_green_skills = list(itertools.chain(*all_green_skills))

        self.next(self.save_data)

    @step
    def save_data(self):
        """Save extracted skills to s3."""
        from datetime import datetime as date
        from dap_prinz_green_jobs.getters.ojo import get_ojo_sample

        ojo_data = get_ojo_sample()
        job_ids = ojo_data.job_id.to_list()

        job_ids if self.production else job_ids[: self.chunk_size]

        job_id_green_skills = dict(zip(job_ids, self.flat_all_green_skills))

        # save extracted green skills to s3
        date_stamp = str(date.today().date()).replace("-", "")

        if self.production:
            logger.info("saving extracted skills to s3...")
            save_to_s3(
                s3,
                BUCKET_NAME,
                job_id_green_skills,
                f"outputs/data/green_skills/{date_stamp}/{self.config_name}_job_adverts.json",
            )

        else:
            pass

        self.next(self.end)

    @step
    def end(self):
        """Ends the flow"""
        pass


if __name__ == "__main__":
    GreenSkillsFlow()
