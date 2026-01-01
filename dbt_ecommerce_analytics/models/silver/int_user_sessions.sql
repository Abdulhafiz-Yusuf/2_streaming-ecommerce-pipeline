/*
Intermediate: Aggregate events by user session
- Session start/end times
- Total events per session
- Session duration
- Conversion indicators
*/

{{ config(
    materialized='view',
    tags=['silver', 'intermediate']
) }}

WITH session_events AS (
    SELECT
        session_id,
        user_id,
        MIN(event_timestamp) as session_start,
        MAX(event_timestamp) as session_end,
        COUNT(*) as total_events,
        COUNT(DISTINCT event_type) as unique_event_types,
        
        -- Event type counts
        COUNT(*) FILTER (WHERE event_type = 'page_view') as page_views,
        COUNT(*) FILTER (WHERE event_type = 'add_to_cart') as add_to_carts,
        COUNT(*) FILTER (WHERE event_type = 'purchase') as purchases,
        
        -- Device/browser (take most common)
        MODE() WITHIN GROUP (ORDER BY device_type) as primary_device,
        MODE() WITHIN GROUP (ORDER BY browser) as primary_browser,
        MODE() WITHIN GROUP (ORDER BY country) as country,
        
        -- Revenue
        SUM(revenue) as session_revenue,
        
        -- Conversion flags
        CASE WHEN COUNT(*) FILTER (WHERE event_type = 'purchase') > 0 
             THEN TRUE ELSE FALSE END as converted,
        CASE WHEN COUNT(*) FILTER (WHERE event_type = 'add_to_cart') > 0 
             THEN TRUE ELSE FALSE END as added_to_cart
        
    FROM {{ ref('stg_clickstream_events') }}
    GROUP BY session_id, user_id
)

SELECT
    session_id,
    user_id,
    session_start,
    session_end,
    EXTRACT(EPOCH FROM (session_end - session_start)) as session_duration_seconds,
    total_events,
    unique_event_types,
    page_views,
    add_to_carts,
    purchases,
    primary_device,
    primary_browser,
    country,
    session_revenue,
    converted,
    added_to_cart,
    
    -- Cart abandonment flag
    CASE 
        WHEN added_to_cart AND NOT converted THEN TRUE 
        ELSE FALSE 
    END as cart_abandoned,
    
    CURRENT_TIMESTAMP as dbt_updated_at
    
FROM session_events