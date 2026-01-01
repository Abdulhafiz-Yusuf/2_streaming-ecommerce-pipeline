/*
Staging: Clean and deduplicate raw bronze events
- Remove duplicate event_ids
- Parse timestamps correctly
- Add derived fields (date, hour, day_of_week)
- Validate event types
*/

{{ config(
    materialized='view',
    tags=['silver', 'staging']
) }}

WITH source_data AS (
    SELECT 
        event_id,
        event_timestamp,
        user_id,
        session_id,
        event_type,
        product_id,
        product_name,
        product_category,
        product_price,
        quantity,
        page_url,
        referrer_url,
        device_type,
        browser,
        country,
        city,
        ingestion_timestamp
    FROM {{ source('bronze', 'clickstream_events') }}
),

-- Deduplicate by event_id (keep first occurrence)
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id 
            ORDER BY ingestion_timestamp ASC
        ) as row_num
    FROM source_data
),

-- Add derived fields
enriched AS (
    SELECT
        -- Original fields
        event_id,
        event_timestamp,
        user_id,
        session_id,
        event_type,
        product_id,
        product_name,
        product_category,
        product_price,
        quantity,
        page_url,
        referrer_url,
        device_type,
        browser,
        country,
        city,
        ingestion_timestamp,
        
        -- Derived timestamp fields
        DATE(event_timestamp) as event_date,
        EXTRACT(HOUR FROM event_timestamp) as event_hour,
        EXTRACT(DOW FROM event_timestamp) as day_of_week,  -- 0=Sunday, 6=Saturday
        TO_CHAR(event_timestamp, 'Day') as day_name,
        
        -- Revenue calculation (only for purchases)
        CASE 
            WHEN event_type = 'purchase' AND quantity IS NOT NULL 
            THEN product_price * quantity 
            ELSE 0 
        END as revenue,
        
        -- Data quality flags
        CASE 
            WHEN event_type IN ('add_to_cart', 'purchase') AND quantity IS NULL 
            THEN TRUE 
            ELSE FALSE 
        END as has_missing_quantity,
        
        -- Processing metadata
        CURRENT_TIMESTAMP as dbt_updated_at
        
    FROM deduped
    WHERE row_num = 1  -- Keep only first occurrence of each event_id
        AND event_type IN ('page_view', 'add_to_cart', 'purchase')  -- Validate event types
)

SELECT * FROM enriched