from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pickle
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from matriculation_forecast.utils import get_model

def generate_predictions(
    table,
    prediction_key,
    bucket_name,
    model_name,
    run_date,
    *args, 
    **kwargs
    ):

    strftime = '%Y-%m-%d'
    now = datetime.strptime(run_date, strftime)
    today = now.strftime(strftime)
    yesterday = (now - timedelta(days=1)).strftime(strftime)
    df = (
        PostgresHook().get_pandas_df(f'select * from {table.schema_in_env}.{table.table_in_env};')
        .assign(
            date=lambda x: pd.to_datetime(x.date),
            cohort_start_date=lambda x: pd.to_datetime(x.cohort_start_date)
            )
        )
    print('today:', today)
    print('yesterday:', yesterday)
    print(df.columns)
    print('df.shape:', df.shape)

    # PREPROCESSING
    scaled_vars = [
    'todo_acad',
    'todo_fin',
    'todo_ea',
    'todo_acad_fin',
    'todo_acad_ea',
    'todo_ea_fin',
    'todo_acad_ea_fin',
    'on_roster_day_1',
    'deferrals'
    ]
    # Scale
    df[scaled_vars] = (
        df[scaled_vars]
        .divide(df.not_enrolled, axis=0)
        )

    df = (
        # Fill division by zero nulls/infs with 
        df.fillna(0) 
        .replace([np.inf, -np.inf], 0)
        # Generate percent_unenrolled variable
        .assign(percent_unenrolled=df.enrolled/df.committed)
        # Calculate true conversion rate
        .rename({'on_roster_day_1': 'conversion_rate'}, axis=1)
        )

    # Model variables
    features = [
        'days_till_start',
        'todo_acad',
        'todo_fin',
        'todo_acad_fin',
        'todo_ea',
        'todo_acad_ea',
        'todo_ea_fin',
        'todo_acad_ea_fin',
        'percent_unenrolled',
        'deferrals',
        ]

    target = 'conversion_rate'

    # Auto-train dataset
    training = df[df.cohort_start_date < today]
    X, y = training[features], training[target]
    print('X.shape:', X.shape)

    # Next cohort prediction data
    inference_frame = df[df.date == yesterday]
    print('inference_frame.shape:', inference_frame.shape)
    
    # Load model
    model = get_model(model_name)

    def generate_prediction(frame, model, features):
        """
        - Receives dataframe and model
        - Predicts the conversion rate for each row
        - Multiplies predicted rate by number of committed-unenrolled students
        - Adds the number of enrolled
        """
        return (np.round(model.predict(frame[features]) * frame.not_enrolled + frame.enrolled).astype(int)).tolist()

    prediction_frame = pd.DataFrame().assign(
        created_at = [datetime.now().strftime(strftime + ' %H:%M:%S')] * len(inference_frame),
        cohort_start_date = inference_frame.cohort_start_date.tolist(),
        days_till_start = inference_frame.days_till_start.tolist(),
        static_prediction = generate_prediction(inference_frame, model, features),
        auto_train_prediction = generate_prediction(inference_frame, model.fit(X, y), features),
    )
    print(inference_frame.cohort_start_date.iloc[0])
    print(today)
    print(prediction_frame)

    S3Hook().load_string(
        prediction_frame.to_csv(index=False),
        prediction_key,
        bucket_name=bucket_name,
        replace=True,
        encrypt=True,
    )

