#!/usr/bin/env bash

# Generate fernet key
FERNET_KEY="$(python3 -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")"

# Store it in the image so that it can be used in entrypoint
echo "$FERNET_KEY" > /usr/local/etc/airflow_fernet_key
