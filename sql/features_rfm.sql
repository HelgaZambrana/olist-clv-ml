WITH ltv AS (
    SELECT
        c.customer_unique_id,
        SUM(o.price) AS total_ltv
    FROM clean.fact_orders o
    JOIN clean.dim_customer c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),
first_purchase AS (
    SELECT
        c.customer_unique_id,
        MIN(o.date_id::timestamp) AS first_purchase_date
    FROM clean.fact_orders o
    JOIN clean.dim_customer c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
)

SELECT
    l.customer_unique_id,
    ('2018-10-17'::date - fp.first_purchase_date::date) AS days_since_first_purchase,
    COUNT(o.order_id) AS frequency,
    SUM(o.price) AS monetary,
    MAX(o.review_score) AS max_review_score,
    AVG(o.review_score) AS avg_review_score,
    l.total_ltv
FROM clean.fact_orders o
JOIN clean.dim_customer c ON o.customer_id = c.customer_id
JOIN ltv l ON c.customer_unique_id = l.customer_unique_id
JOIN first_purchase fp ON c.customer_unique_id = fp.customer_unique_id
WHERE o.order_status = 'delivered'
GROUP BY l.customer_unique_id, fp.first_purchase_date, l.total_ltv