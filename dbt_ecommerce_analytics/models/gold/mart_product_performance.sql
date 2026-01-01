/*
Product Analytics: Best/worst performers
*/

{{ config(
    materialized='table',
    tags=['gold', 'products']
) }}

SELECT
    product_id,
    product_name,
    product_category,
    product_price,
    
    -- Engagement metrics
    COUNT(*) FILTER (WHERE event_type = 'page_view') as total_views,
    COUNT(*) FILTER (WHERE event_type = 'add_to_cart') as total_adds_to_cart,
    COUNT(*) FILTER (WHERE event_type = 'purchase') as total_purchases,
    
    -- Revenue metrics
    SUM(quantity) FILTER (WHERE event_type = 'purchase') as units_sold,
    SUM(revenue) as total_revenue,
    
    -- Conversion rates
    ROUND(100.0 * COUNT(*) FILTER (WHERE event_type = 'add_to_cart') / 
          NULLIF(COUNT(*) FILTER (WHERE event_type = 'page_view'), 0), 2) as view_to_cart_rate,
    ROUND(100.0 * COUNT(*) FILTER (WHERE event_type = 'purchase') / 
          NULLIF(COUNT(*) FILTER (WHERE event_type = 'add_to_cart'), 0), 2) as cart_to_purchase_rate,
    
    -- Ranking
    RANK() OVER (ORDER BY SUM(revenue) DESC) as revenue_rank,
    RANK() OVER (ORDER BY SUM(quantity) FILTER (WHERE event_type = 'purchase') DESC) as units_rank,
    
    CURRENT_TIMESTAMP as dbt_updated_at
    
FROM {{ ref('stg_clickstream_events') }}
GROUP BY product_id, product_name, product_category, product_price
HAVING COUNT(*) FILTER (WHERE event_type = 'purchase') > 0  -- Only products with sales
ORDER BY total_revenue DESC