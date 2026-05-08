# Olist CLV-ML: Customer Lifetime Value Prediction

Regression model to predict the expected monetary value of a customer
based on first purchase behavior, using the Brazilian E-Commerce Public 
Dataset by Olist.

## Business Questions

- What is the expected monetary value of a new customer based on their first purchase?
- How much should be invested in acquiring a new customer given their profile?

## Model

Regression problem — predicting customer Lifetime Value (LTV) based on 
first purchase behavior.

**Models compared:**
- Linear Regression (baseline)
- Decision Tree Regressor
- Random Forest Regressor
- XGBoost Regressor (main model)

**Optimization:** GridSearchCV for hyperparameter tuning

**Metrics:** MAE, RMSE, R²

## Stack

- **Python** — ETL and modeling
- **PostgreSQL / Neon** — cloud database
- **scikit-learn / XGBoost** — modeling

## Architecture

```
data/raw/        ← original CSVs (not tracked by git)
data/clean/      ← processed outputs (not tracked by git)
src/             ← ETL and modeling scripts
sql/             ← schema definitions
notebooks/       ← EDA and model development
```

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

## Features

**Original RFM features:**
- `days_since_first_purchase` — recency proxy
- `frequency` — number of orders
- `log_monetary` — log-transformed total spend (StandardScaler applied)
- `avg_review_score` — customer satisfaction signal

**Enriched features (first purchase signals):**
- `product_category_encoded` — category of highest-value product in first purchase (Target Encoding)
- `payment_installments` — number of installments chosen (StandardScaler applied)
- `payment_type_*` — payment method one-hot encoded (boleto, credit_card, debit_card, voucher)

**Target variable:** `log_ltv` — log-transformed total customer lifetime value

**Note:** Initial analysis showed 1.0 correlation between `monetary` and `total_ltv` 
for single-purchase customers (97% of dataset). Enriched features were added to 
introduce independent behavioral signals from the first purchase beyond raw spend value.