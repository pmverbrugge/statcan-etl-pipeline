from dotenv import load_dotenv
import os

env_path = os.path.join(os.path.dirname(__file__), '..', 'scripts', '.env')
load_dotenv(dotenv_path=os.path.abspath(env_path))

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

