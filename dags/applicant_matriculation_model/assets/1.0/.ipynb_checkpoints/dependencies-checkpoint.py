import re
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.compose._column_transformer import ColumnTransformer


Interval = pd._libs.interval.Interval

class ApplicantModel(LogisticRegression):

    bands = [
            Interval(left=0, right=0.0488),
            Interval(left=0.0488, right=0.0898),
            Interval(left=0.0898, right=0.154),
            Interval(left=0.154, right=0.237),
            Interval(left=0.237, right=1)
        ]

    def predict(self, X, interval=False):
        probabilities = self.predict_proba(X)[:, 1]

        scores = []
        for prob in probabilities:
            for idx, band in enumerate(self.bands, start=1):
                if prob > band.left and prob <= band.right:
                    if interval:
                        scores.append(band)
                    else:
                        scores.append(idx)
        return scores


class ApplicationImputer(SimpleImputer):

    def transform(self, X):
        transformed = super().transform(X)
        return pd.DataFrame(transformed, columns=X.columns)


class ImputerTransformDataFrame(ColumnTransformer):

    _feature_names_in = ['application_referral_source', # App Query
             'similarity', # Python
             'interested_in_full_time_cohort', # App query
             'application_discipline', # App Query
             'professional_background_word_count', # App query
             'paid_non_paid', # App query
             'ten_minute_chat', # App query
             'coding_languages_mentioned', # Python
             'free_resources_mentioned', # Python
             'application_pacing', # App Query
             'applied_for_scholarship', # App Query
             'technical_background_word_count', # App Query
             'downloaded_outcomes', # App query
             'education', # App Query
             'applicant_location', # App Query
             'lower_i', # App Query
             'professional_goal' # App Query
             ]

    def transform(self, X):
        transformed = super().transform(X)
        return pd.DataFrame(transformed, columns=self.transformers[0][2])

    def fit_transform(self, X, y=None):
        transformed = super().fit_transform(X, y)
        return pd.DataFrame(transformed, columns=self.transformers[0][2])


class EncodeLocation(OneHotEncoder):

    locations = ['New York-Northern New Jersey-Long Island, NY-NJ-PA MSA',
                 'Washington-Arlington-Alexandria, DC-VA-MD-WV MSA',
                 'Denver-Aurora, CO MSA',
                 'Houston-Sugar Land-Baytown, TX MSA',
                 'Chicago-Naperville-Joliet, IL-IN-WI MSA',
                 'Austin-Round Rock, TX MSA',
                 'San Francisco-Oakland-Fremont, CA MSA',
                 'Seattle-Tacoma-Bellevue, WA MSA',
                 'Atlanta-Sandy Springs-Marietta, GA MSA',
                 'Los Angeles-Long Beach-Santa Ana, CA MSA',
                 'Dallas-Fort Worth-Arlington, TX MSA',
                 'Philadelphia-Camden-Wilmington, PA-NJ-DE-MD MSA',
                 'Miami-Fort Lauderdale-Pompano Beach, FL MSA',
                 'Detroit-Warren-Livonia, MI MSA',
                 'Colorado Springs, CO MSA',
                 'Phoenix-Mesa-Scottsdale, AZ MSA',
                 'San Diego-Carlsbad-San Marcos, CA MSA',
                 'Tampa-St. Petersburg-Clearwater, FL',
                 'Portland-Vancouver-Beaverton, OR-WA MSA',
                 'Columbus, OH MSA',
                 'Riverside-San Bernardino-Ontario, CA MSA',
                 'Minneapolis-St. Paul-Bloomington, MN-WI MSA',
                 'Las Vegas-Paradise, NV MSA',
                 'Charlotte-Gastonia-Concord, NC-SC MSA',
                 'Orlando-Kissimmee, FL MSA',
                 'Baltimore-Towson, MD MSA',
                 'St. Louis, MO-IL MSA',
                 'Pittsburgh, PA MSA',
                 'Nashville-Davidson-Murfreesboro-Franklin, TN MSA',
                 'Cleveland-Elyria-Mentor, OH MSA',
                 'San Antonio, TX MSA',
                 'Birmingham-Hoover, AL MSA',
                 ]

    def fit(self, X, y=None):
        X_ = self.replace(X)
        super().fit(X_, y)
        return self

    def transform(self, X, y=None):
        X_ = self.replace(X)
        return super().transform(X_)

    def replace(self, X):
        X_ = X.copy()
        X_[~np.in1d(X_, self.locations)] = 'Other'
        # Originally, the `check_if_none` functions was not
        # used, and instead `X_ == None` was used to generate
        # a mast of True and False values for filtering.
        # Flake8 doesn't allow `value == None` however.
        # Flake8 requires None comparisons to be `value is None`
        # The `is` operator is not supported for numpy element wise checks
        # ie: `array is None` returns a single boolean.
        check_if_none = np.vectorize(lambda x: x is None)
        X_[check_if_none(X_)] = 'Other'
        return X_


class EncodeFullTimeInterest(OneHotEncoder):

    r = re.compile('Yes|No')

    def fit(self, X, y=None):
        X_ = self.encode(X)
        super().fit(X_)
        return self

    def transform(self, X):
        X_ = self.encode(X)
        return super().transform(X_)

    def encode(self, X):
        missing = 'NULL'

        def find_yes_no(x):
            if not x:
                return missing
            match = self.r.findall(x)
            if not match:
                return missing
            return match[0]
        vmatch = np.vectorize(find_yes_no, otypes=[str])
        X_ = vmatch(X)
        return X_
