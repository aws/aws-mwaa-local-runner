import pickle
import sys
from pathlib import Path

cwd = Path(__file__).resolve().parent
sys.path.append(cwd.as_posix())

print(sys.path)

from dependencies import *

model_path = Path(__file__).resolve().parent / 'model__1.0.pkl'
with open(model_path.as_posix(), 'rb') as file:
    MODEL = pickle.load(file)
