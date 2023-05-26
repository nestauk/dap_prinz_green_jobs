import pandas as pd
from dap_prinz_green_jobs.getters.data_getters import load_s3_data
from dap_prinz_green_jobs import BUCKET_NAME
from typing import Dict, List


def get_green_skills_taxonomy() -> pd.DataFrame:
    """Load ESCO's green skills taxonomy from s3

    Returns:
        pd.DataFrame: ESCO's green skills taxonomy
    """
    return load_s3_data(
        BUCKET_NAME, "outputs/data/green_skill_lists/green_esco_data_formatted.csv"
    )


def get_green_skills_taxonomy_embeddings() -> Dict[str, List[float]]:
    """Loads ESCO's green skills taxonomy embeddings from s3

    Returns:
        Dict[str, np.ndarray]: A dictionary where the key is the green skill id
            and the value is a list of floats representing the embedding
    """
    return load_s3_data(
        BUCKET_NAME, "outputs/data/green_skill_lists/green_esco_embeddings.json"
    )
