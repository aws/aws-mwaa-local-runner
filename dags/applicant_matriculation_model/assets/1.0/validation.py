"""
This file should contain code for validating data prior to
passing the data to the model.

This file must contain a `main` function that receives two arguments:
1. data - The data that is being validated
2. task_name - The name of the dag

This function must activate all validation code.

Why have a module for validation when we are already
doing preprocessing in the model pipeline?

Dropping rows of data is not something that is supported
and managed by sklearn preprocessing and pipeline tools
and is easiest to do prior to activating an sklearn pipeline.
"""

import json
import pandas as pd
from pathlib import Path
from datetime import datetime


def main(data, task_name):
    validation = get_validation()
    cat_report, cat_drop_idx = validate_categorical_features(
        data,
        validation['categorical'],
        task_name,
        record_id='id'
        )
    num_report, num_drop_idx = validate_numeric_features(
        data,
        validation['numerical'],
        task_name,
        record_id='id'
        )
    print('\n\n\n', cat_report[0] if cat_report else 'No Categorical Errors Encountered')
    data_ = data.drop(cat_drop_idx + num_drop_idx)
    if cat_report or num_report:
        error_report = pd.DataFrame(cat_report + num_report).assign(details=lambda x: x.details.apply(json.dumps))
    else:
        error_report = pd.DataFrame()
    
    return data_, error_report

def get_validation():
    """
    Returns a dictionary with two keys:
        1. categorical
            - The value is the json data for validating categorical predictors
        2. numerical
            - The value is the json data for validating numerical predictors
    """
    # Get path for parent directory of this file
    pardir = Path(__file__).resolve().parent
    # Map the top level key to the correct json filepath
    paths = dict(
        categorical=(pardir / 'lib' / 'categorical_validation.json').as_posix(),
        numerical=(pardir / 'lib' / 'numerical_validation.json').as_posix(),
    )

    # Final validation dataset
    data = {}
    for path in paths:
        with open(paths[path], 'r') as file:
            data[path] = json.load(file)

    return data


def validate_categorical_features(
        data: pd.DataFrame,
        validation_json: dict,
        task_name: str,
        record_id: str = 'id'):
    """
    Parameters:

    data - pandas dataframe. The modeling data that needs to be validated
           The dataframe must have a column for every column listed in the
           validation_json variable

    validation_json - dictionary. The json used to validated each column

    task_name - The name of the modeling task

    record_id - The name of the unique id column for each row

    Returns:

    A list of dictionaries containing logging information for ever rows that
    is found to be "corrupt".

    A list of index values for each corrupt row in the data.

    """
    # Get the names of each categorical column
    categorical_columns = validation_json.keys()
    # The final report where information for each
    # corrupt row will be stored
    report = []
    # The indices we will drop from the dataframe
    drop_ids = set()

    for column in categorical_columns:
        # The categories the model has been trained on
        expected_values = validation_json[column]['values']
        # Indicates whether or not null values should be filled
        fill = validation_json[column]['nulls']['fill']

        # Filter down to rows containing unknown categories
        errors = data[~data[column].isin(expected_values)][[record_id, column]]

        if fill:
            # If we are filling null values for the column
            # Then we do not consider a null value to be corrupt data.
            # Here we remove null rows from the errors dataframe
            null_index = data[data[column].isna()].index.tolist()
            errors = errors.drop([x for x in errors.index if x in null_index])

        if errors.shape[0] > 0:
            # Convert each row to a report dictionary
            errors = errors.apply(lambda x: {
                            'record_id': x[record_id],
                            'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'task_name': task_name,
                            'kind': 'unknown_categorical_value',
                            'details': {
                'variable': column,
                'value': 'NULL' if pd.isna(x[column]) else x[column]
                }
                                
                            }, axis=1)
            # Add the errors to the report dictionary
            report += errors.tolist()
            # Add the index values for each corrupt row
            # to the drop_ids set
            drop_ids.update(errors.index.tolist())

    return report, list(drop_ids)


def validate_numeric_features(data: pd.DataFrame,
                              validation_json: dict,
                              task_name: str,
                              record_id: str = 'id',
                              drop_range_errors=False):
    """
    Parameters:

    data - pandas dataframe. The modeling data that needs to be validated
           The dataframe must have a column for every column listed in the
           validation_json variable

    validation_json - dictionary. The json used to validated each column

    task_name - The name of the modeling task

    record_id - The name of the unique id column for each row

    drop_range - Bool. default: False
                 Indicates whether or not datapoints that fall
                 outside the range the model was trained on are considered
                 corrupt. If False, logs will be generated for outliers
                 but the rows will not be dropped from the dataset

    Returns:

    A list of dictionaries containing logging information for ever rows that
    is found to be "corrupt".

    A list of index values for each corrupt row in the data.
    """
    # Collect the numeric column names
    numeric_columns = validation_json.keys()
    # The final report objects
    report = []
    # The object containing the indices containing corrupt data
    drop_ids = set()

    for column in numeric_columns:
        # Indicates if we indent to fill null values for the column
        fill = validation_json[column]['nulls']['fill']
        # The min and max values the model was trained on
        range_ = validation_json[column]['range']
        # Isolate the unique id and the column we are validating
        frame = data[[record_id, column]]
        if fill:
            # If we are filling nulls for the column
            # we do not consider nulls corrupt data
            # Here we drop rows where the column's value is null
            frame = frame.dropna(subset=[column])
        frame = frame.assign(
            # Convert all values in the column to numeric
            # errors='coerce' sets all failed conversions to np.nan
            converted=pd.to_numeric(frame[column], errors='coerce'),
            # If converted=Null, the data likely contains alphabetic characters
            # and is considered corrupt.
            conversion_errors=lambda x: x.converted.isna(),
            # Here we replace null values with the minimum values
            # in order to check for unexpected outliers in the next line
            imputed=lambda x: x.converted.fillna(range_['min']),
            # Check if the value falls outside the range the model was trained on
            range_errors=lambda x: ~((x.imputed >= range_['min']) & (x.imputed <= range_['max']))
        )
        # Isolate all rows where there was either a conversion error or a range error
        errors = frame.query('(conversion_errors==True) or (range_errors==True)')

        # A function to be applied to each row of the errors dataframe.
        # And generates a report for the row
        def create_report(x):
            error_report = {
                'record_id': x[record_id],
                'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'task_name': task_name,
                'kind': '',
                'details': {
                     'variable': column,
                     'value': x[column],
                     'min': range_['min'],
                     'max': range_['max']}
                }
            if x.conversion_errors:
                error_type = {"kind": "failed_numeric_conversion"}
            elif x.range_errors:
                error_type = {"kind": "numeric_unexpected_value"}
            error_report.update(error_type)
            return error_report

        if errors.shape[0] > 0:
            # Add the error reports to the final report list
            report += errors.apply(create_report, axis=1).tolist()

            if not drop_range_errors:
                # Collect indices for all error types except 'numeric_unexpected_value'
                conversion_ids = [x['record_id'] for x in report if x['kind'] != 'numeric_unexpected_value']
                # Filter numeric_unexpected_value errors from the errors dataframe
                errors = errors.query(f'{record_id}.isin({conversion_ids})')
            # Add corrupt data rows indices to the drop_ids set
            drop_ids.update(errors.index.tolist())

    return report, list(drop_ids)
