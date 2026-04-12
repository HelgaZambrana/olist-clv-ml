import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

engine = create_engine(os.getenv("DATABASE_URL"))

def test_connection():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version()"))
        print(result.fetchone())

if __name__ == "__main__":
    test_connection()