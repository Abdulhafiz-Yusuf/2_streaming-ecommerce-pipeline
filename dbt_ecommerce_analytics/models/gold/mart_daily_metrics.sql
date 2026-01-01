/*
Business Metrics: Daily performance dashboard
Key metrics executives care about
*/

{{ config(
    materialized='table',
    tags=['gold', 'daily']
) }}

WITH daily_events AS (
    SELECT
        event_date,
        COUNT(*) as total_events,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT session_id) as total_sessions,
        
        -- Event breakdown
        COUNT(*) FILTER (WHERE event_type = 'page_view') as page_views,
        COUNT(*) FILTER (WHERE event_type = 'add_to_cart') as add_to_carts,
        COUNT(*) FILTER (WHERE event_type = 'purchase') as purchases,
        
        -- Revenue metrics
        SUM(revenue) as total_revenue,
        AVG(revenue) FILTER (WHERE revenue > 0) as avg_order_value,
        
        -- Product metrics
        COUNT(DISTINCT product_id) FILTER (WHERE event_type = 'purchase') as products_sold,
        SUM(quantity) FILTER (WHERE event_type = 'purchase') as units_sold
        
    FROM {{ ref('stg_clickstream_events') }}
    GROUP BY event_date
),

session_metrics AS (
    SELECT
        DATE(session_start) as event_date,
        COUNT(*) FILTER (WHERE converted) as converted_sessions,
        COUNT(*) FILTER (WHERE cart_abandoned) as abandoned_carts,
        AVG(session_duration_seconds) as avg_session_duration_seconds
    FROM {{ ref('int_user_sessions') }}
    GROUP BY DATE(session_start)
)

SELECT
    e.event_date,
    e.total_events,
    e.unique_users,
    e.total_sessions,
    e.page_views,
    e.add_to_carts,
    e.purchases,
    e.total_revenue,
    e.avg_order_value,
    e.products_sold,
    e.units_sold,
    
    -- Session metrics
    s.converted_sessions,
    s.abandoned_carts,
    s.avg_session_duration_seconds,
    
    -- Conversion rates (%)
    ROUND(100.0 * e.add_to_carts / NULLIF(e.page_views, 0), 2) as cart_rate,
    ROUND(100.0 * e.purchases / NULLIF(e.add_to_carts, 0), 2) as checkout_rate,
    ROUND(100.0 * e.purchases / NULLIF(e.page_views, 0), 2) as overall_conversion_rate,
    ROUND(100.0 * s.abandoned_carts / NULLIF(e.total_sessions, 0), 2) as cart_abandonment_rate,
    
    -- Per-user metrics
    ROUND(e.total_revenue / NULLIF(e.unique_users, 0), 2) as revenue_per_user,
    ROUND(e.page_views::NUMERIC / NULLIF(e.unique_users, 0), 2) as avg_pages_per_user,
    
    CURRENT_TIMESTAMP as dbt_updated_at
    
FROM daily_events e
LEFT JOIN session_metrics s ON e.event_date = s.event_date
ORDER BY e.event_date DESC