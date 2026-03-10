-- sql/transformation.sql
-- Mock transformation: revenue summary per customer region and product category
-- This is the query whose output we want to validate against the Talend reference

SELECT
    c.region,
    c.segment,
    p.category,
    COUNT(s.sale_id)                                            AS total_orders,
    SUM(s.quantity)                                             AS total_units,
    ROUND(
        SUM(s.quantity * p.unit_price * (1 - s.discount)), 2
    )                                                           AS total_revenue,
    ROUND(
        AVG(s.quantity * p.unit_price * (1 - s.discount)), 2
    )                                                           AS avg_order_value
FROM
    fact_sales      s
    JOIN dim_customer c ON s.customer_id = c.customer_id
    JOIN dim_product  p ON s.product_id  = p.product_id
GROUP BY
    c.region,
    c.segment,
    p.category
ORDER BY
    c.region,
    c.segment,
    p.category;