#!/bin/bash
set -e

# Initialize the Airflow database if not already initialized
airflow db migrate

# Execute the command passed to the container
exec "$@"
