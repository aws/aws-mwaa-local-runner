import pickle
from pathlib import Path

MODEL_PATH = Path(__file__).resolve().parent / '2023-02-01__conversion_lr.pkl'

with open(MODEL_PATH, 'rb') as file:
    MODEL = pickle.load(file)