-- ============================================================
-- CAPA CLEAN: star schema listo para análisis y modelo ML
-- ============================================================

CREATE SCHEMA IF NOT EXISTS clean;

-- ------------------------------------------------------------
-- DIMENSIONES
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS clean.dim_customer (
    customer_id             TEXT PRIMARY KEY,
    customer_unique_id      TEXT NOT NULL,
    customer_city           TEXT,
    customer_state          TEXT
);

CREATE TABLE IF NOT EXISTS clean.dim_product (
    product_id                      TEXT PRIMARY KEY,
    product_category_name           TEXT,
    product_category_name_english   TEXT,
    product_weight_g                NUMERIC,
    product_length_cm               NUMERIC,
    product_height_cm               NUMERIC,
    product_width_cm                NUMERIC
);

CREATE TABLE IF NOT EXISTS clean.dim_seller (
    seller_id       TEXT PRIMARY KEY,
    seller_city     TEXT,
    seller_state    TEXT
);

CREATE TABLE IF NOT EXISTS clean.dim_date (
    date_id         TEXT PRIMARY KEY,
    full_date       TIMESTAMP,
    year            INTEGER,
    month           INTEGER,
    day             INTEGER,
    quarter         INTEGER,
    day_of_week     INTEGER
);

-- ------------------------------------------------------------
-- FACT TABLE
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS clean.fact_orders (
    order_id                TEXT PRIMARY KEY,
    customer_id             TEXT REFERENCES clean.dim_customer(customer_id),
    date_id                 TEXT REFERENCES clean.dim_date(date_id),
    price                   NUMERIC,
    freight_value           NUMERIC,
    payment_value           NUMERIC,
    payment_type            TEXT,
    payment_installments    INTEGER,
    review_score            INTEGER,
    order_status            TEXT
);


CREATE TABLE IF NOT EXISTS clean.features (
    customer_unique_id          TEXT PRIMARY KEY,
    days_since_first_purchase   INTEGER,
    frequency                   INTEGER,
    monetary                    NUMERIC,
    max_review_score            INTEGER,
    avg_review_score            NUMERIC,
    total_ltv                   NUMERIC
);