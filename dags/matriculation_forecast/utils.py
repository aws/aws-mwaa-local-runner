from pathlib import Path
import importlib

ASSETS = Path(__file__).resolve().parent / 'assets'

def get_model(model=None):
    if not model:
        model = max(ASSETS.iterdir())
    elif isinstance(model, str):
        model = ASSETS / model
    module = import_module(f'.assets.{model.stem}')
    return module.MODEL
    
def import_module(path):
    return importlib.import_module(path, package='matriculation_forecast')

