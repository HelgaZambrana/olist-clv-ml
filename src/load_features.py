import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

engine = create_engine(os.getenv("DATABASE_URL"))

def load_features():
    with open("sql/features_rfm.sql", "r") as f:
        sql = f.read()
    
    df = pd.read_sql(sql, engine)
    
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE clean.features"))
        conn.commit()
    
    df.to_sql(
        name="features",
        con=engine,
        schema="clean",
        if_exists="append",
        index=False
    )
    print(f"features: {len(df)} filas cargadas")

if __name__ == "__main__":
    load_features()