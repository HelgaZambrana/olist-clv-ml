import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

engine = create_engine(os.getenv("DATABASE_URL"))

def create_schema():
    with open("sql/schema_raw.sql", "r") as f:
        sql = f.read()
    
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
        print("Schema creado correctamente.")

if __name__ == "__main__":
    create_schema()