import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

engine = create_engine(os.getenv("DATABASE_URL"))

def truncate_tables():
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE TABLE clean.fact_orders CASCADE"))
        conn.execute(text("TRUNCATE TABLE clean.dim_customer CASCADE"))
        conn.execute(text("TRUNCATE TABLE clean.dim_product CASCADE"))
        conn.execute(text("TRUNCATE TABLE clean.dim_seller CASCADE"))
        conn.execute(text("TRUNCATE TABLE clean.dim_date CASCADE"))
        conn.commit()
    print("Tablas clean truncadas")

def load_table(df, table_name):
    df.to_sql(
        name=table_name,
        con=engine,
        schema="clean",
        if_exists="append",
        index=False
    )
    print(f"{table_name}: {len(df)} filas cargadas")

def etl_dim_customer():
    df = pd.read_sql("""
        SELECT 
            customer_id,
            customer_unique_id,
            customer_city,
            customer_state
        FROM raw.customers
    """, engine)
    load_table(df, "dim_customer")

def etl_dim_product():
    df = pd.read_sql("""
        SELECT 
            p.product_id,
            p.product_category_name,
            t.product_category_name_english,
            CAST(p.product_weight_g AS NUMERIC) AS product_weight_g,
            CAST(p.product_length_cm AS NUMERIC) AS product_length_cm,
            CAST(p.product_height_cm AS NUMERIC) AS product_height_cm,
            CAST(p.product_width_cm AS NUMERIC) AS product_width_cm
        FROM raw.products p
        LEFT JOIN raw.product_category_translation t
            ON p.product_category_name = t.product_category_name
    """, engine)
    load_table(df, "dim_product")

def etl_dim_seller():
    df = pd.read_sql("""
        SELECT
            seller_id,
            seller_city,
            seller_state
        FROM raw.sellers
    """, engine)
    load_table(df, "dim_seller")

def etl_dim_date():
    df = pd.read_sql("""
        SELECT DISTINCT order_purchase_timestamp
        FROM raw.orders
        WHERE order_purchase_timestamp IS NOT NULL
    """, engine)
    df["full_date"] = pd.to_datetime(df["order_purchase_timestamp"])
    df["date_id"] = df["full_date"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["year"] = df["full_date"].dt.year
    df["month"] = df["full_date"].dt.month
    df["day"] = df["full_date"].dt.day
    df["quarter"] = df["full_date"].dt.quarter
    df["day_of_week"] = df["full_date"].dt.dayofweek
    df = df[["date_id","full_date","year","month","day","quarter","day_of_week"]]
    load_table(df, "dim_date")

def etl_fact_orders():
    df = pd.read_sql("""
        SELECT
            o.order_id,
            o.customer_id,
            o.order_purchase_timestamp       AS date_id,
            SUM(CAST(i.price AS NUMERIC))           AS price,
            SUM(CAST(i.freight_value AS NUMERIC))   AS freight_value,
            MAX(CAST(p.payment_value AS NUMERIC))   AS payment_value,
            MAX(p.payment_type)                     AS payment_type,
            MAX(CAST(p.payment_installments AS INTEGER)) AS payment_installments,
            MAX(CAST(r.review_score AS INTEGER))    AS review_score,
            o.order_status
        FROM raw.orders o
        LEFT JOIN raw.order_items i      ON o.order_id = i.order_id
        LEFT JOIN raw.order_payments p   ON o.order_id = p.order_id
        LEFT JOIN raw.order_reviews r    ON o.order_id = r.order_id
        GROUP BY
            o.order_id,
            o.customer_id,
            o.order_purchase_timestamp,
            o.order_status
    """, engine)
    load_table(df, "fact_orders")

if __name__ == "__main__":
    truncate_tables()
    etl_dim_customer()
    etl_dim_product()
    etl_dim_seller()
    etl_dim_date()
    etl_fact_orders()