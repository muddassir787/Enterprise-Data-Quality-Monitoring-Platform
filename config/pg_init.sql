-- Create users
CREATE USER airflow WITH PASSWORD 'airflow_secure_pass';
CREATE USER ge_user WITH PASSWORD 'ge_secure_pass';
CREATE USER warehouse_user WITH PASSWORD 'warehouse_secure_pass';

-- Create databases with proper ownership
CREATE DATABASE airflow OWNER airflow;
CREATE DATABASE ge_store OWNER ge_user;
CREATE DATABASE warehouse_db OWNER warehouse_user;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
GRANT ALL PRIVILEGES ON DATABASE ge_store TO ge_user;
GRANT ALL PRIVILEGES ON DATABASE warehouse_db TO warehouse_user;

-- Connect to the warehouse database to set up schemas and tables
\c warehouse_db

-- Create schema and table
CREATE SCHEMA IF NOT EXISTS warehouse;
ALTER SCHEMA warehouse OWNER TO warehouse_user;

CREATE TABLE IF NOT EXISTS warehouse.sales_transactions (
    transaction_id VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(100) NOT NULL,
    product_category VARCHAR(50) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    transaction_date TIMESTAMP NOT NULL,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE warehouse.sales_transactions OWNER TO warehouse_user;

-- Connect to the Great Expectations store to initialize any database configuration if needed
\c ge_store

-- (GE will automatically create its tables when it runs database stores, 
-- but we grant all schema creation permissions to ge_user just in case)
GRANT ALL PRIVILEGES ON DATABASE ge_store TO ge_user;
