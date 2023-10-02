import os
from shared.irontable import Irontable


def sql_templates_path():
    return os.path.join(os.path.dirname(__file__), "sql")


def load_docs():
    doc_path = os.path.join(os.path.dirname(__file__), "README.md")
    with open(doc_path, 'r') as docs:
        return '\n'.join(docs.readlines()[:-1])
