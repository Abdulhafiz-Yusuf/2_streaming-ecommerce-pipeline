/*
Conversion Funnel Analysis
Shows drop-off at each stage of user journey
*/

{{ config(
    materialized='table',
    tags=['gold', 'funnel']
) }}

WITH funnel_stages AS (
    SELECT
        COUNT(DISTINCT session_id) as total_sessions,
        COUNT(DISTINCT session_id) FILTER (WHERE page_views > 0) as sessions_with_views,
        COUNT(DISTINCT session_id) FILTER (WHERE add_to_carts > 0) as sessions_with_carts,
        COUNT(DISTINCT session_id) FILTER (WHERE purchases > 0) as sessions_with_purchases
    FROM {{ ref('int_user_sessions') }}
)

SELECT
    'Stage 1: Visited Site' as funnel_stage,
    1 as stage_order,
    total_sessions as sessions,
    total_sessions as users_remaining,
    0 as users_dropped,
    100.0 as conversion_rate_from_previous,
    100.0 as conversion_rate_from_start
FROM funnel_stages

UNION ALL

SELECT
    'Stage 2: Viewed Products' as funnel_stage,
    2 as stage_order,
    sessions_with_views as sessions,
    sessions_with_views as users_remaining,
    total_sessions - sessions_with_views as users_dropped,
    ROUND(100.0 * sessions_with_views / NULLIF(total_sessions, 0), 2) as conversion_rate_from_previous,
    ROUND(100.0 * sessions_with_views / NULLIF(total_sessions, 0), 2) as conversion_rate_from_start
FROM funnel_stages

UNION ALL

SELECT
    'Stage 3: Added to Cart' as funnel_stage,
    3 as stage_order,
    sessions_with_carts as sessions,
    sessions_with_carts as users_remaining,
    sessions_with_views - sessions_with_carts as users_dropped,
    ROUND(100.0 * sessions_with_carts / NULLIF(sessions_with_views, 0), 2) as conversion_rate_from_previous,
    ROUND(100.0 * sessions_with_carts / NULLIF(total_sessions, 0), 2) as conversion_rate_from_start
FROM funnel_stages

UNION ALL

SELECT
    'Stage 4: Completed Purchase' as funnel_stage,
    4 as stage_order,
    sessions_with_purchases as sessions,
    sessions_with_purchases as users_remaining,
    sessions_with_carts - sessions_with_purchases as users_dropped,
    ROUND(100.0 * sessions_with_purchases / NULLIF(sessions_with_carts, 0), 2) as conversion_rate_from_previous,
    ROUND(100.0 * sessions_with_purchases / NULLIF(total_sessions, 0), 2) as conversion_rate_from_start
FROM funnel_stages

ORDER BY stage_order