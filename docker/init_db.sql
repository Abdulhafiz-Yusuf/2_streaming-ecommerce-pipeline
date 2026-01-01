-- Bronze schema: Raw events land here
CREATE SCHEMA IF NOT EXISTS bronze;

-- Raw clickstream events table
CREATE TABLE IF NOT EXISTS bronze.clickstream_events (
    event_id VARCHAR(50) PRIMARY KEY,
    event_timestamp TIMESTAMP NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    session_id VARCHAR(50) NOT NULL,
    event_type VARCHAR(20) NOT NULL,  -- page_view, add_to_cart, purchase, etc.
    product_id VARCHAR(50),
    product_name VARCHAR(200),
    product_category VARCHAR(100),
    product_price DECIMAL(10,2),
    quantity INTEGER,
    page_url TEXT,
    referrer_url TEXT,
    device_type VARCHAR(20),
    browser VARCHAR(50),
    country VARCHAR(50),
    city VARCHAR(100),
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster queries
CREATE INDEX idx_event_timestamp ON bronze.clickstream_events(event_timestamp);
CREATE INDEX idx_user_id ON bronze.clickstream_events(user_id);
CREATE INDEX idx_event_type ON bronze.clickstream_events(event_type);

-- Silver and Gold schemas (dbt will create tables here)
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;