"""
Script to train and use a classifier to predict green or not-green from a skill text
Will also output the closest green ESCO skill if green is predicted
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.metrics import precision_recall_fscore_support
from sklearn.metrics.pairwise import cosine_similarity
import spacy

from dap_prinz_green_jobs.utils.bert_vectorizer import get_embeddings
from dap_prinz_green_jobs.getters.data_getters import load_s3_data, save_to_s3
from dap_prinz_green_jobs.getters.skill_getters import get_green_skills_taxonomy
from dap_prinz_green_jobs import BUCKET_NAME, logger
from dap_prinz_green_jobs.getters.occupation_getters import (
    load_onet_green_topics,
)

from tqdm import tqdm
import pickle
from typing import List, Union, Tuple
import joblib
import s3fs
from datetime import datetime
import os

OJO_BUCKET_NAME = "open-jobs-lake"


def find_green_topics(skill_ent: str, green_topics: list) -> list:
    """
    Find the green topics which are contained within a skill entity using exact matching
    Args:
        skill_ent: A skill entity
        green_topics: A list of green topics
    Returns:
        list: A list of the green topics found in this skill entity (can be empty)
    Example:
        find_green_topics("We want someone with an interest in sustainability", ["Sustainability", "Green Energy"])
        returns ["Sustainability"]
    """
    found_green_topics = []
    for green_topic in green_topics:
        # Basic cleaning
        green_topic = green_topic.lower()
        clean_skill_ent = skill_ent.lower()
        clean_skill_ent = " ".join(clean_skill_ent.split())
        if green_topic in clean_skill_ent:
            found_green_topics.append(green_topic)
    return found_green_topics


def process_green_topic_data(nlp):
    """
    Function to load the ONET green topic data, clean it, add to it

    Args:
        nlp: Spacy model used to lemmatise the green topic

    Returns:
        list: The new processed list of ONET green topics
    """

    green_topics = load_onet_green_topics()

    green_topics = green_topics["Topic"].unique()

    # Lower case green topics, and lemmatised versions of them too
    enhanced_green_topics = set()
    for green_topic in green_topics:
        green_topic = green_topic.lower()
        enhanced_green_topics.add(green_topic)
        sent = nlp(green_topic)
        enhanced_green_topics.add(" ".join(word.lemma_ for word in sent))

    # Manual additions from green topics which are more wordy and can be split up
    enhanced_green_topics.add(
        "environmental health"
    )  # 'environmental health and safety (ehs)'
    enhanced_green_topics.add(
        "environmental safety"
    )  # 'environmental health and safety (ehs)'
    enhanced_green_topics.add(
        "leadership in energy design"
    )  # 'leadership in energy and environmental design ( leed )'
    enhanced_green_topics.add(
        "leadership in environmental design"
    )  # 'leadership in energy and environmental design ( leed )'
    enhanced_green_topics.add(
        "solid waste management"
    )  # 'solid waste management , treatment , and reduction'
    enhanced_green_topics.add(
        "solid waste treatment"
    )  # 'solid waste management , treatment , and reduction'
    enhanced_green_topics.add(
        "solid waste reduction"
    )  # 'solid waste management , treatment , and reduction'
    enhanced_green_topics.add("urban planning")  # 'urban and regional planning'
    enhanced_green_topics.add("regional planning")  # 'urban and regional planning'
    enhanced_green_topics.add(
        "wastewater management"
    )  # 'wastewater management , treatment , and reduction'
    enhanced_green_topics.add(
        "wastewater treatment"
    )  # 'wastewater management , treatment , and reduction'
    enhanced_green_topics.add(
        "wastewater reduction"
    )  # 'wastewater management , treatment , and reduction'

    enhanced_green_topics = list(enhanced_green_topics)

    return enhanced_green_topics


def get_closest_match(
    skills_list_embeddings_dict: dict,
    green_data_embeddings_dict: dict,
    formatted_green_data: pd.DataFrame(),
) -> dict:
    """
    Find the semantically closest matches for each skill in skills_list_embeddings_dict
    using a green data set and their associated embeddings
    Args:
        skills_list_embeddings_dict: The skill to the skill embedding
        green_data_embeddings_dict: The green data embeddings
        formatted_green_data: Must have a "description" and a "id" column

        Note: the key of green_data_embeddings_dict must be associated with the index of formatted_green_data
        i.e. The embedding for formatted_green_data.iloc[4] is in green_data_embeddings_dict[4]

    Returns:
        dict: gives the skill to the green data match information (if any found)
    """

    similarities = cosine_similarity(
        np.array(list(skills_list_embeddings_dict.values())),
        np.array(list(green_data_embeddings_dict.values())),
    )
    # Top matches for skill chunk
    top_green_skills = get_green_skill_matches(
        extracted_skill_list=list(skills_list_embeddings_dict.keys()),
        similarities=similarities,
        green_skills_taxonomy=formatted_green_data,
        skill_threshold=0,
    )

    return {sk[0]: sk[1] for sk in top_green_skills if sk[1]}


def get_green_skill_matches(
    extracted_skill_list: List[str],
    similarities: np.array,
    green_skills_taxonomy: pd.DataFrame(),
    skill_threshold: float = 0.7,
) -> List[Tuple[str, Tuple[str, int]]]:
    """Get green skill matches for a list of extracted skills - use this
            in extract_green_measures flow instead of get_green_skill_measures

            NOTE: this is because speeds up skills mapping considerably
            and because the esco green taxonomy is not hierarchical so we are simply
            matching the extracted skills to the green taxonomy based on a minimum
            threshold cosine similarity.

    Args:
            extracted_skill_list (List[str]): List of extracted skills

    Returns:
            List[Tuple[str, Tuple[str, int, int]]]: List of tuples with the extracted
                            skill; the mapped green skill, a green skill id and the match similarity score
    """
    skill_top_green_skills = []
    for skill_ix, skill in tqdm(enumerate(extracted_skill_list)):
        top_skill_match = np.flip(np.sort(similarities[skill_ix]))[0:1]
        if top_skill_match[0] > skill_threshold:
            green_skill_ix = np.flip(np.argsort(similarities[skill_ix]))[0:1]
            green_skill = green_skills_taxonomy.iloc[green_skill_ix].description.values[
                0
            ]
            green_skill_id = green_skills_taxonomy.iloc[green_skill_ix].id.values[0]
            skill_top_green_skills.append(
                (skill, (green_skill, green_skill_id, top_skill_match[0]))
            )
        else:
            skill_top_green_skills.append((skill, None))

    return skill_top_green_skills


class GreenSkillClassifier(object):
    def __init__(self):
        logger.info("Downloading ONET green topics")
        nlp = spacy.load("en_core_web_sm")
        self.enhanced_green_topics = process_green_topic_data(nlp)
        # Format into a format needed for get_green_skill_matches()
        self.formatted_green_topics = (
            pd.DataFrame({"description": self.enhanced_green_topics})
            .reset_index()
            .rename(columns={"index": "id"})
        )

        logger.info("Embedding ONET green topics")
        self.enhanced_green_topics_embeddings_dict = get_embeddings(
            self.enhanced_green_topics
        )

    def load_training_data(
        self,
        training_data_path: str = "inputs/data/training_data/green_skill_training_data.csv",
    ) -> pd.DataFrame:
        """
        Download the training data and clean it a little
        """
        all_data = load_s3_data(
            BUCKET_NAME,
            training_data_path,
        )

        all_data["green?"] = all_data["green?"].map(
            {"TRUE": "green", "FALSE": "not_green"}
        )
        data = all_data[all_data["green?"].isin(["green", "not_green"])]

        return data

    def load_esco_data(self):
        logger.info("Downloading ESCO green taxonomy embeddings")
        self.taxonomy_skills_embeddings_dict = load_s3_data(
            BUCKET_NAME,
            "outputs/data/green_skill_lists/green_esco_embeddings_20230815.json",
        )

        logger.info("Downloading ESCO green taxonomy")
        self.formatted_taxonomy = get_green_skills_taxonomy()

    def transform(
        self,
        skill_entity_list: Union[str, List[str]],
        skills_list_embeddings_dict: Union[None, list] = None,
    ):
        """
        Transform a list of skill entities into 3 numerical features;
        1. The closest similarity score between the skill entity and a ESCO green skill
        2. The closest similarity score between the skill entity and a ONET green topic
        3. The number of ONET green topics exactly found in the skill entity text

        Args:
            skill_entity_list (str or list of strings): A skill entity text or a list of them
            skills_list_embeddings_dict (None or list): The embeddings for the skill entity if they have already been calculated
        Returns:
            list: For every skill entity inputted, 3 numerical features are calculated and outputted in a list
            dict: A dictionary of unique skill entities to which closest green ESCO skill they match to

        """

        if isinstance(skill_entity_list, str):
            skill_entity_list = [skill_entity_list]

        if not skills_list_embeddings_dict:
            skills_list_embeddings_dict = get_embeddings(skill_entity_list)

        # Get the similarity to the closest ESCO green skill
        all_extracted_green_skills_dict = get_closest_match(
            skills_list_embeddings_dict,
            self.taxonomy_skills_embeddings_dict,
            self.formatted_taxonomy,
        )

        # Get the similarity to the closest green topic
        green_skills_green_topics_dict = get_closest_match(
            skills_list_embeddings_dict,
            self.enhanced_green_topics_embeddings_dict,
            self.formatted_green_topics,
        )

        # Return esco_score, green_topic_score, num_topics for each skill in skill_entity_list

        return [
            [
                all_extracted_green_skills_dict.get(skill_ent)[2],
                green_skills_green_topics_dict.get(skill_ent)[2],
                len(find_green_topics(skill_ent, self.enhanced_green_topics)),
            ]
            for skill_ent in skill_entity_list
        ], all_extracted_green_skills_dict

    def fit(self, X_train: Union[np.array, list], y_train: Union[np.array, list]):
        """
        Fit a random forest classifier to the training data
        Args:
            X_train: Training features
            y_train: Test data

        Returns:
            sklearn.pipeline.Pipeline
        """

        self.model = make_pipeline(
            StandardScaler(),
            RandomForestClassifier(
                n_estimators=500, random_state=42, class_weight="balanced"
            ),
        )

        self.model = self.model.fit(X_train, y_train)
        return self.model

    def fit_transform(
        self, skill_entity_list: list, y_train: Union[np.array, list]
    ) -> list:
        """
        Fit a random forest classifier to skill entities
        Args:
            skill_entity_list: A list of skill entities
            y_train: Test data

        Returns:
            The transformed feature data
        """

        skills_list_transform, _ = self.transform(skill_entity_list)

        self.model = self.fit(skills_list_transform, y_train)

        return skills_list_transform

    def predict(
        self,
        skill_entity_list: Union[str, List[str]],
        skills_list_embeddings_dict: Union[None, list] = None,
        output_match: bool = True,
    ) -> list:
        """
        Predict "green" or "not_green" for a list of skill entities
        output_match = True to output more information about the top ESCO green skill match

        Args:
            skill_entity_list: A skill entity or a list of skill entities
            skills_list_embeddings_dict (None or list): The embeddings for the skill entity if they have already been calculated
            output_match: Whether to output the top ESCO green skill match (True) or not (False)

        Returns:
            list: The predictions of whether the input skills are green or not

        """

        if isinstance(skill_entity_list, str):
            skill_entity_list = [skill_entity_list]

        skills_list_transform, all_extracted_green_skills_dict = self.transform(
            skill_entity_list, skills_list_embeddings_dict=skills_list_embeddings_dict
        )

        class_pred = list(self.model.predict(skills_list_transform))
        if output_match:
            # ("green", 0.97, [top_esco_match, top_esco_match_id, top_esco_match_score])
            class_pred_prob = list(
                np.max(self.model.predict_proba(skills_list_transform), axis=1)
            )
            y_pred = list(
                zip(
                    class_pred,
                    class_pred_prob,
                    [
                        all_extracted_green_skills_dict[skill_ent]
                        for skill_ent in skill_entity_list
                    ],
                )
            )
        else:
            y_pred = self.model.predict(skills_list_transform)

        return y_pred

    def evaluate(self, X_test, y_test):
        y_preds = green_skills_classifier.predict(X_test["ent"].tolist())
        self.results = classification_report(
            y_test, [p[0] for p in y_preds], output_dict=True
        )
        return self.results

    def save(self, output_file, results_dict=None):
        logger.info(f"Saving the model to {output_file}")
        fs = s3fs.S3FileSystem()

        with fs.open(os.path.join(f"s3://{BUCKET_NAME}", output_file), "wb") as f:
            joblib.dump(self.model, f)

        if results_dict:
            save_to_s3(
                BUCKET_NAME,
                results_dict,
                output_file.split(".joblib")[0] + "_results.json",
            )

    def load(
        self,
        model_file="s3://prinz-green-jobs/outputs/models/green_skill_classifier/green_skill_classifier_20230906.joblib",
    ):
        logger.info(f"Loading the model from {model_file}")
        fs = s3fs.S3FileSystem()

        with fs.open(model_file, "rb") as f:
            self.model = joblib.load(f)


if __name__ == "__main__":
    green_skills_classifier = GreenSkillClassifier()

    data = green_skills_classifier.load_training_data()
    green_skills_classifier.load_esco_data()
    X_train, X_test, y_train, y_test = train_test_split(
        data, data["green?"], test_size=0.25, random_state=42
    )

    _ = green_skills_classifier.fit_transform(X_train["ent"].tolist(), y_train)
    green_skills_classifier.predict("A skill about sustainable development")

    test_results = green_skills_classifier.evaluate(X_test, y_test)

    date = datetime.now().strftime("%Y-%m-%d").replace("-", "")

    output_file = (
        f"outputs/models/green_skill_classifier/green_skill_classifier_{date}.joblib"
    )
    green_skills_classifier.save(output_file, results_dict=test_results)
