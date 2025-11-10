-- sql/load_redshift_tables.sql
-- Replace the IAM role ARN and the date
COPY analytics.fact_sales
FROM 's3://<BUCKET>/clean/orders/dt=2025-11-01/'
IAM_ROLE '<REDSHIFT_ROLE_ARN>'
FORMAT AS PARQUET;
