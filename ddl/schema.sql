-- ddl/schema.sql
-- Mock data model for Talend-to-SQL migration validation

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id   INTEGER PRIMARY KEY,
    customer_name TEXT    NOT NULL,
    region        TEXT    NOT NULL,
    segment       TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id    INTEGER PRIMARY KEY,
    product_name  TEXT    NOT NULL,
    category      TEXT    NOT NULL,
    unit_price    REAL    NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id       INTEGER PRIMARY KEY,
    customer_id   INTEGER NOT NULL,
    product_id    INTEGER NOT NULL,
    sale_date     TEXT    NOT NULL,
    quantity      INTEGER NOT NULL,
    discount      REAL    NOT NULL DEFAULT 0.0,
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (product_id)  REFERENCES dim_product(product_id)
);