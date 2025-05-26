# This module is called from ETL scripts to obtain database credentials

from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="/app/scripts/.env")

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

