#!/usr/bin/env bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install --user -r requirement.txt
fi 

if [ -f "$REQUIREMENTS_FILE" ]; then
  pip install --no-cache-dir -r "$REQUIREMENTS_FILE"
fi

# Initialize / upgrade DB
airflow db upgrade

# Create admin user only if it does not exist
airflow users list | grep -q admin || \
airflow users create \
  --username admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com \
  --password admin

exec airflow webserver
