# Plugins Folder

Place your custom Airflow plugins in this directory.

## Usage

1. Create your plugin files in this directory
2. They will be automatically loaded by Airflow
3. Add any Python dependencies to `../requirements/requirements.txt`

## Example Plugin Structure

```
plugins/
├── my_plugin.py
└── operators/
    └── my_operator.py
```

