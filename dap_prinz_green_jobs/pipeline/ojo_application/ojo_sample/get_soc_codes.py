"""
python dap_prinz_green_jobs/pipeline/ojo_application/ojo_sample/get_soc_codes.py run
"""
from dap_prinz_green_jobs.pipeline.ojo_application.ojo_sample.ojo_sample_utils import (
    get_soc4_codes,
    desired_sample_size,
    random_seed,
)
from dap_prinz_green_jobs import BUCKET_NAME

from metaflow import FlowSpec, step, Parameter
import pandas as pd


class OjoSocFlow(FlowSpec):
    """Flow to extract soc4 codes for all unique job titles."""

    production = Parameter("production", help="Run in production?", default=True)
    random_seed = Parameter("random_seed", help="Random seed", default=random_seed)
    sample_size = Parameter(
        "sample_size", help="Sample size", default=desired_sample_size
    )
    chunk_size = Parameter("chunk_size", help="Chunk size", default=11000)

    @step
    def start(self):
        """
        Starts the flow. Loads SOC mapper
        """
        from dap_prinz_green_jobs.pipeline.green_measures.occupations.soc_map import (
            SOCMapper,
        )

        self.soc_mapper = SOCMapper()
        self.soc_mapper.load()

        self.next(self.load_data)

    @step
    def load_data(self):
        """Loads relevant OJO data"""
        from dap_prinz_green_jobs import config

        print("loading datasets...")
        self.deduplicated_ids = pd.read_csv(
            "s3://prinz-green-jobs/outputs/data/ojo_application/deduplicated_sample/deduplicated_job_ids.csv"
        )
        self.deduplicated_ids_list = self.deduplicated_ids.id.to_list()

        job_titles = pd.read_parquet(config["ojo_s3_file_adverts_ojd_daps_extract"])
        # There are duplicate rows with different date formats
        self.job_titles = job_titles.drop_duplicates(
            subset=job_titles.columns.difference(["created"])
        )
        # load job locations
        job_locations = pd.read_parquet(config["ojo_s3_file_locations"])
        self.job_locations = job_locations.drop_duplicates()

        self.next(self.process_data)

    @step
    def process_data(self):
        """Merge datasets"""
        from toolz import partition_all

        print("processing dataset...")
        self.job_title_locations = (
            pd.merge(self.job_titles, self.job_locations, on="id")
            .query("id in @self.deduplicated_ids_list")
            .query("is_uk == 1")
            .query("is_large_geo == 0")[
                [
                    "id",
                    "job_title_raw",
                    "itl_2_code",
                    "itl_2_name",
                    "itl_3_code",
                    "itl_3_name",
                ]
            ]
            .reset_index(drop=True)
        )

        self.unique_job_titles = (
            self.job_title_locations.job_title_raw.unique().tolist()
        )
        self.unique_job_titles = (
            self.unique_job_titles if self.production else self.unique_job_titles[:100]
        )

        print(
            f"there are {len(self.unique_job_titles)} unique job titles to extract SOC codes for..."
        )

        self.job_title_chunks = list(
            partition_all(self.chunk_size, self.unique_job_titles)
        )

        self.next(self.extract_soc, foreach="job_title_chunks")

    @step
    def extract_soc(self):
        """Extract SOC codes for unique job titles"""
        self.soc_codes = self.soc_mapper.get_soc(job_titles=self.input)

        self.next(self.join)

    @step
    def join(self, inputs):
        """
        Joins the outputs of the previous step
        """
        import itertools

        print("merging soc codes...")
        all_soc_codes = list(itertools.chain(*[inputs.soc_codes for inputs in inputs]))

        print("getting soc4 codes...")
        soc4_codes = get_soc4_codes(all_soc_codes)

        print("getting all job titles...")
        all_job_titles = list(
            itertools.chain(*[inputs.unique_job_titles for inputs in inputs])
        )

        print("creating job title to soc4 code mapping...")
        self.jobtitles2soc = dict(zip(all_job_titles, soc4_codes))

        self.next(self.end)

    @step
    def end(self):
        """
        ends flow
        """
        from dap_prinz_green_jobs.getters.data_getters import save_to_s3

        print("saving soc4 codes to s3...")
        save_to_s3(
            BUCKET_NAME,
            self.jobtitles2soc,
            f"outputs/data/ojo_application/deduplicated_sample/jobtitles2soc4_production_{str(self.production).lower()}.json",
        )


if __name__ == "__main__":
    OjoSocFlow()
