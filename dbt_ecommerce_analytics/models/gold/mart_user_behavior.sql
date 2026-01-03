WITH user_aggregates AS (
    SELECT
        user_id,
        
        -- Session metrics
        COUNT(DISTINCT session_id) as total_sessions,
        MIN(session_start) as first_session_date,
        MAX(session_start) as last_session_date,
        
        -- Calculate days active (time span between first and last session)
        EXTRACT(DAY FROM (MAX(session_start) - MIN(session_start))) + 1 as days_active,
        
        -- Event counts
        SUM(total_events) as total_events,
        SUM(page_views) as total_page_views,
        SUM(add_to_carts) as total_add_to_carts,
        SUM(purchases) as total_purchases,
        
        -- Revenue metrics
        SUM(session_revenue) as lifetime_revenue,
        AVG(session_revenue) as avg_revenue_per_session,
        MAX(session_revenue) as highest_order_value,
        
        -- Engagement metrics
        AVG(session_duration_seconds) as avg_session_duration_seconds,
        AVG(total_events) as avg_events_per_session,
        
        -- Conversion indicators
        COUNT(*) FILTER (WHERE converted = TRUE) as converted_sessions,
        COUNT(*) FILTER (WHERE cart_abandoned = TRUE) as abandoned_sessions,
        
        -- Device preference (most common device used)
        MODE() WITHIN GROUP (ORDER BY primary_device) as preferred_device,
        MODE() WITHIN GROUP (ORDER BY primary_browser) as preferred_browser,
        MODE() WITHIN GROUP (ORDER BY country) as country
        
    FROM {{ ref('int_user_sessions') }}
    GROUP BY user_id
),

user_segments_base AS (
    SELECT
        user_id,
        total_sessions,
        first_session_date,
        last_session_date,
        days_active,
        total_events,
        total_page_views,
        total_add_to_carts,
        total_purchases,
        lifetime_revenue,
        avg_revenue_per_session,
        highest_order_value,
        avg_session_duration_seconds,
        avg_events_per_session,
        converted_sessions,
        abandoned_sessions,
        preferred_device,
        preferred_browser,
        country,
        
        -- Conversion rate
        ROUND(100.0 * converted_sessions / NULLIF(total_sessions, 0), 2) as user_conversion_rate,
        
        -- Cart abandonment rate
        ROUND(100.0 * abandoned_sessions / NULLIF(total_sessions, 0), 2) as user_abandonment_rate,
        
        -- Average order value (for users who purchased)
        CASE 
            WHEN total_purchases > 0 
            THEN ROUND(lifetime_revenue / total_purchases, 2) 
            ELSE 0 
        END as avg_order_value,
        
        -- Recency (days since last session)
        EXTRACT(DAY FROM (CURRENT_TIMESTAMP - last_session_date)) as days_since_last_session,
        
        -- Frequency bucket (session frequency)
        CASE
            WHEN total_sessions >= 10 THEN 'Power User (10+ sessions)'
            WHEN total_sessions >= 5 THEN 'Regular User (5-9 sessions)'
            WHEN total_sessions >= 2 THEN 'Casual User (2-4 sessions)'
            ELSE 'One-Time Visitor (1 session)'
        END as frequency_segment,
        
        -- Monetary bucket (total spend)
        CASE
            WHEN lifetime_revenue >= 500 THEN 'High Value (₦825k+)'
            WHEN lifetime_revenue >= 200 THEN 'Medium Value (₦330k-₦825k)'
            WHEN lifetime_revenue >= 50 THEN 'Low Value (₦82.5k-₦330k)'
            WHEN lifetime_revenue > 0 THEN 'Micro Value (<₦82.5k)'
            ELSE 'Non-Buyer'
        END as monetary_segment
        
    FROM user_aggregates
),

user_segments_enriched AS (
    SELECT
        *,
        
        -- Recency bucket (days since last activity) - NOW using the computed column
        CASE
            WHEN days_since_last_session <= 1 THEN 'Active (Today/Yesterday)'
            WHEN days_since_last_session <= 7 THEN 'Recent (This Week)'
            WHEN days_since_last_session <= 30 THEN 'Warm (This Month)'
            ELSE 'Cold (30+ days ago)'
        END as recency_segment,
        
        -- Engagement level (based on avg events per session)
        CASE
            WHEN avg_events_per_session >= 10 THEN 'Highly Engaged'
            WHEN avg_events_per_session >= 5 THEN 'Moderately Engaged'
            WHEN avg_events_per_session >= 2 THEN 'Low Engagement'
            ELSE 'Very Low Engagement'
        END as engagement_level,
        
        -- User lifecycle stage - NOW using the computed column
        CASE
            WHEN total_purchases >= 3 AND days_since_last_session <= 30 THEN 'Loyal Customer'
            WHEN total_purchases >= 1 AND days_since_last_session <= 30 THEN 'Active Buyer'
            WHEN total_purchases >= 1 AND days_since_last_session > 30 THEN 'Churned Buyer'
            WHEN abandoned_sessions > 0 AND days_since_last_session <= 7 THEN 'Hot Lead (Recent Cart Abandonment)'
            WHEN abandoned_sessions > 0 AND days_since_last_session > 7 THEN 'Cold Lead'
            WHEN total_page_views > 0 AND days_since_last_session <= 7 THEN 'Active Browser'
            ELSE 'Inactive'
        END as lifecycle_stage,
        
        -- Purchase propensity score (0-100)
        LEAST(100, ROUND(
            (total_page_views * 1.0) +           -- 1 point per page view
            (total_add_to_carts * 5.0) +         -- 5 points per add to cart
            (total_purchases * 20.0) +            -- 20 points per purchase
            (CASE WHEN days_since_last_session <= 7 THEN 10 ELSE 0 END) -- 10 bonus points for recent activity
        , 0)) as purchase_propensity_score
        
    FROM user_segments_base
),

rfm_scores AS (
    SELECT
        user_id,
        
        -- RFM Score Components (1-5 scale, 5 being best)
        NTILE(5) OVER (ORDER BY days_since_last_session ASC) as recency_score,  -- Lower days = better
        NTILE(5) OVER (ORDER BY total_sessions) as frequency_score,
        NTILE(5) OVER (ORDER BY lifetime_revenue) as monetary_score
        
    FROM user_segments_enriched
),

final AS (
    SELECT
        u.*,
        
        -- Add RFM scores
        r.recency_score,
        r.frequency_score,
        r.monetary_score,
        
        -- Combined RFM score (5-5-5 is best customer)
        CONCAT(r.recency_score, '-', r.frequency_score, '-', r.monetary_score) as rfm_segment,
        
        -- Overall customer score (sum of RFM, max 15)
        (r.recency_score + r.frequency_score + r.monetary_score) as customer_score,
        
        -- Customer tier based on RFM
        CASE
            WHEN (r.recency_score + r.frequency_score + r.monetary_score) >= 13 THEN 'Champion'
            WHEN (r.recency_score + r.frequency_score + r.monetary_score) >= 10 THEN 'Loyal'
            WHEN (r.recency_score + r.frequency_score + r.monetary_score) >= 7 THEN 'Potential'
            WHEN (r.recency_score + r.frequency_score + r.monetary_score) >= 5 THEN 'At Risk'
            ELSE 'Lost'
        END as customer_tier,
        
        CURRENT_TIMESTAMP as dbt_updated_at
        
    FROM user_segments_enriched u
    LEFT JOIN rfm_scores r USING (user_id)
)

SELECT * FROM final
ORDER BY customer_score DESC, lifetime_revenue DESC