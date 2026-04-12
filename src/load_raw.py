import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

engine = create_engine(os.getenv("DATABASE_URL"))

FILES = {
    "customers":                    "olist_customers_dataset.csv",
    "orders":                       "olist_orders_dataset.csv",
    "order_items":                  "olist_order_items_dataset.csv",
    "order_payments":               "olist_order_payments_dataset.csv",
    "order_reviews":                "olist_order_reviews_dataset.csv",
    "products":                     "olist_products_dataset.csv",
    "sellers":                      "olist_sellers_dataset.csv",
    "geolocation":                  "olist_geolocation_dataset.csv",
    "product_category_translation": "product_category_name_translation.csv",
}

def load_raw():
    for table, filename in FILES.items():
        path = os.path.join("data/raw", filename)
        df = pd.read_csv(path, dtype=str)
        df.columns = df.columns.str.strip('"')
        df.to_sql(
            name=table,
            con=engine,
            schema="raw",
            if_exists="replace",
            index=False
        )
        print(f"{table}: {len(df)} filas cargadas")

if __name__ == "__main__":
    load_raw()