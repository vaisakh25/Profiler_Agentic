"""
Load all WWI CSV files into PostgreSQL under the 'wwi' schema.
Each CSV becomes a table named after the file (lowercased, without extension).
"""

import os
import glob
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("POSTGRES_HOST", "localhost")
PORT = os.getenv("POSTGRES_PORT", "5432")
USER = os.getenv("POSTGRES_USER", "profiler")
PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB = os.getenv("POSTGRES_DB", "profiler")
SCHEMA = "wwi"

CSV_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "files", "wwi_files")

def main():
    url = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"
    engine = create_engine(url)

    # Create schema if it doesn't exist
    with engine.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
        conn.commit()
    print(f"Schema '{SCHEMA}' ready.")

    csv_files = glob.glob(os.path.join(CSV_DIR, "*.csv"))
    print(f"Found {len(csv_files)} CSV files to load.\n")

    for csv_path in sorted(csv_files):
        table_name = os.path.splitext(os.path.basename(csv_path))[0].lower()
        print(f"Loading {os.path.basename(csv_path)} -> {SCHEMA}.{table_name} ... ", end="")
        try:
            df = pd.read_csv(csv_path)
            df.to_sql(
                name=table_name,
                con=engine,
                schema=SCHEMA,
                if_exists="replace",
                index=False,
            )
            print(f"{len(df)} rows")
        except Exception as e:
            print(f"FAILED: {e}")

    print("\nDone! All WWI tables loaded into PostgreSQL.")

if __name__ == "__main__":
    main()
