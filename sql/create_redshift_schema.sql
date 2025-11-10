-- sql/create_redshift_schema.sql
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dim_customer (
  customer_id VARCHAR(64) PRIMARY KEY,
  gender VARCHAR(10),
  signup_date DATE,
  region VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS analytics.dim_product (
  product_id VARCHAR(64) PRIMARY KEY,
  product VARCHAR(255),
  product_category VARCHAR(128),
  brand VARCHAR(128)
);

CREATE TABLE IF NOT EXISTS analytics.fact_sales (
  order_id VARCHAR(128) PRIMARY KEY,
  order_ts TIMESTAMP,
  order_date DATE,
  customer_id VARCHAR(64),
  product VARCHAR(255),
  product_category VARCHAR(128),
  sales NUMERIC(12,2),
  quantity INT,
  discount NUMERIC(12,2),
  profit NUMERIC(12,2),
  shipping_cost NUMERIC(12,2),
  payment_method VARCHAR(64),
  device_type VARCHAR(64),
  profit_margin NUMERIC(8,4),
  aging_bucket VARCHAR(32)
);
