import pickle
import sys
from pathlib import Path

cwd = Path(__file__).resolve().parent
sys.path.append(cwd.as_posix())


MODEL_PATH = Path(__file__).resolve().parent / '2023-03-14__conversion_lr.pkl'

    
with open(MODEL_PATH, 'rb') as file:
    MODEL = pickle.load(file)