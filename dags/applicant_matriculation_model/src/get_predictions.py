import pandas as pd
from pathlib import Path
from datetime import datetime
from applicant_matriculation_model.src.utils import get_model

def get_predictions(data, version=None, columns=None):
    """
    The main function for loading and activating the model.

    This function
    1. Imports dependencies (airflow required all imports inside the function)
    2. Defines a function for loading a module with an absolute path
        - This is done because airflow moves all files into a temporary directory
          when a dag is activated, and relative imports are discouraged.
        - The absolute path expected to passed to get_predictions is the absolute path
          to the src/utils.py file, where code exists for loading the model
    3. Loads the model using the provided version number
    4. Passed `data` into  `.predict` and `.predict_proba` methods
    5. Returns the data as a dataframe
    """

    columns = [
      'application_referral_source',
      'similarity',
      'interested_in_full_time_cohort',
      'application_discipline',
      'professional_background_word_count',
      'paid_non_paid',
      'ten_minute_chat',
      'coding_languages_mentioned',
      'free_resources_mentioned',
      'application_pacing',
      'applied_for_scholarship',
      'technical_background_word_count',
      'downloaded_outcomes',
      'education',
      'applicant_location',
      'lower_i',
      'professional_goal'
      ]

    # Load model
    model = get_model(version=version)
    model.steps[-2][1]._feature_names_in = columns
    # PREDICTIONS
    probs = model.predict_proba(data[columns] if columns else data)[:, 1]
    scores = model.predict(data[columns] if columns else data)
    # PREDICTIONS DATASET
    frame = pd.DataFrame().assign(
        record_id=data.id,
        record_type='application',
        model_name='applicant_matriculation',
        model_version=str(version),
        prediction=probs,
        score=scores,
        create_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    return frame
