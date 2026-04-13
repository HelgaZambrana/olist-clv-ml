# Olist CLV-ML: Customer Retention Model

Binary classification model to predict customer repurchase probability 
using the Brazilian E-Commerce Public Dataset by Olist.

## Business Questions

- What is the expected monetary value of a new customer based on their first purchase?
- How much should be invested in acquiring a new customer given their profile?

## Model

Regression problem — predicting customer Lifetime Value (LTV) based on 
first purchase behavior.

- **Baseline:** Linear Regression
- **Main model:** XGBoost Regressor  
- **Metrics:** MAE, RMSE, R²

## Stack

- **Python** — ETL and modeling
- **PostgreSQL / Neon** — cloud database
- **scikit-learn / XGBoost** — modeling (TBD)
- **Jupyter Notebooks** — analysis and EDA (pending)

## Architecture
data/raw/        ← original CSVs (not tracked by git)
data/clean/      ← processed outputs
src/             ← ETL and modeling scripts
sql/             ← schema definitions
notebooks/       ← EDA and model development

Two-layer database architecture:
- `raw` schema — exact replica of source CSVs, no transformations
- `clean` schema — star schema optimized for analytics and ML

## Database Design Decisions

**Fact table granularity:** `fact_orders` is modeled at the order level, 
not the item level, because the unit of analysis for the model is the customer. 
This is a deliberate design decision, not an oversight.

**Payment data simplification:** `payment_type` and `payment_installments` 
are taken from the highest-value payment per order (MAX). In a production 
system, a separate payments table would be more appropriate, but this 
simplification is justified given the scope of the project.

**Geolocation:** Raw lat/lng data was excluded from the clean layer due to 
its non-unique zip code structure (1M+ rows, multiple coordinates per prefix). 
Only `city` and `state` are retained in `dim_customer` and `dim_seller`.

## Production Notes

In a production environment, this pipeline would be orchestrated with 
Apache Airflow or dbt, with scheduled runs and automatic retries.