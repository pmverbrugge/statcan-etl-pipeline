import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import TreebankWordTokenizer
from collections import defaultdict
from loguru import logger
import psycopg2
from statcan.tools.config import DB_CONFIG

nltk.download("punkt")
nltk.download("stopwords")
nltk.download("wordnet")

logger.add("/app/logs/member_base_name_normalizer.log", rotation="1 MB", retention="7 days")

lemmatizer = WordNetLemmatizer()
stop_words = set(stopwords.words("english"))
tokenizer = TreebankWordTokenizer()

def normalize_label(text):
    tokens = tokenizer.tokenize(str(text).lower())
    tokens = [t for t in tokens if t.isalpha() and t not in stop_words]
    return "_".join(sorted(set(tokens)))  # deterministic group key

def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)


def assign_base_names():
    logger.info("🔍 Normalizing member labels to derive base_names...")

    with get_db_conn() as conn:
        member_df = pd.read_sql("""
            SELECT dimension_hash, member_id, member_name_en
            FROM dictionary.dimension_set_member
        """, conn)

        member_df["base_name"] = member_df["member_name_en"].apply(normalize_label)

        logger.info("🧩 Updating base_name in dictionary.dimension_set_member...")
        cur = conn.cursor()
        for _, row in member_df.iterrows():
            cur.execute("""
                UPDATE dictionary.dimension_set_member
                SET base_name = %s
                WHERE dimension_hash = %s AND member_id = %s
            """, (row["base_name"], row["dimension_hash"], row["member_id"]))

        conn.commit()
        logger.info("✅ base_name assignment complete.")


if __name__ == "__main__":
    try:
        assign_base_names()
    except Exception as e:
        logger.error(f"❌ Failed to assign base_names: {e}")

