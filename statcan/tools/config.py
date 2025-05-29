from dotenv import load_dotenv
import os

env_path = os.path.join(os.path.dirname(__file__), '..', 'scripts', '.env')
load_dotenv(dotenv_path=os.path.abspath(env_path))

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

