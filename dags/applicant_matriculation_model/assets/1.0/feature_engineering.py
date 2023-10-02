import json
import re
import numpy as np
from pathlib import Path
from difflib import SequenceMatcher


def main(data):
    return engineer_features(data)


def engineer_features(df):
    df_ = df.copy()
    key_terms = get_key_terms()
    learning = key_terms['learning']
    technical = key_terms['learning']
    df_ = df_.assign(free_resources_mentioned=(
                     df_.technical_background
                        .str.lower()
                        .fillna('')
                        .apply(count_key_terms, terms=learning)
                     + df_.professional_background
                        .str.lower()
                        .fillna('')
                        .apply(count_key_terms, terms=learning)
                     ),
                     coding_languages_mentioned=(
                     df_.technical_background
                        .str.lower()
                        .fillna('')
                        .apply(count_key_terms, terms=technical)
                     + df_.professional_background
                        .str.lower()
                        .fillna('')
                        .apply(count_key_terms, terms=technical)
                     ),
                     similarity=(df_[['technical_background',
                                      'professional_background']].astype(str)
                                 .apply(lambda x: text_similarity(x.technical_background,
                                                                  x.professional_background,
                                                                  ),
                                        axis=1)
                                 ),
                     lower_i=(df.professional_background
                              + ' ' + df.technical_background)
                     .apply(lambda x: int(' i ' in str(x)))
                     )
    # Pandas has their own null type that is not
    # consistently recognized by sklearn null imputers
    # Sklearn imputers allow passing pd.NA as an argument
    # to resolve this, but the version of pandas currently used
    # by Irondata does not have an NA attribute.
    # Long story short, here we convert pandas null objects
    # to np.nan which works with sklearn imputers.
    df_ = df_.fillna(np.nan)

    return df_


def count_key_terms(text, terms):
    pattern = compile_key_term_pattern(terms)
    matches = set(pattern.findall(text))
    return len(matches)


def text_similarity(text1, text2):
    return SequenceMatcher(None,
                           text1.lower() if text1 else '',
                           text2.lower() if text2 else '').ratio()


def get_key_terms():
    pardir = Path(__file__).resolve().parent
    paths = dict(
        learning=(pardir / 'lib' / 'learning_resources.json').as_posix(),
        technical=(pardir / 'lib' / 'technical_terms.json').as_posix()
        )
    data = {}
    for path in paths:
        with open(paths[path], 'r') as file:
            data[path] = json.load(file)

    return data


def compile_key_term_pattern(terms):
    """
    terms - An iterable of key terms

    returns - A compiled regex pattern for identifing the presents of each term.
              The pattern is primarily just joining the iterable together with the
              `|` character
    """
    template = r'\b({terms})\b'
    pattern = template.format(terms='|'.join(([re.escape(x) for x in terms])))
    return re.compile(pattern)
