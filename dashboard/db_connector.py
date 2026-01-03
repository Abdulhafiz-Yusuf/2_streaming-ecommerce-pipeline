"""
Database connector with connection pooling
Efficiently manages PostgreSQL connections for dashboard
"""

import psycopg2
from psycopg2 import pool
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres123")
POSTGRES_DB = os.getenv("POSTGRES_DB")


class DatabaseConnector:
    """
    Manages PostgreSQL connections with pooling.
    
    Why connection pooling?
    - Reuse connections instead of creating new ones (faster)
    - Limit concurrent connections (prevent database overload)
    - Auto-reconnect on failures
    """
    
    def __init__(self):
        try:
            # Create connection pool (min 1, max 5 connections)
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, 5,
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            
            if self.connection_pool:
                print("✅ Database connection pool created successfully")
                
        except Exception as e:
            print(f"❌ Error creating connection pool: {e}")
            raise
    
    def execute_query(self, query, params=None):
        """
        Execute SQL query and return results as DataFrame.
        
        Args:
            query (str): SQL query to execute
            params (tuple): Query parameters for safe substitution
            
        Returns:
            pd.DataFrame: Query results
        """
        connection = None
        try:
            # Get connection from pool
            connection = self.connection_pool.getconn()
            
            # Execute query and return as DataFrame
            df = pd.read_sql_query(query, connection, params=params)
            
            return df
            
        except Exception as e:
            print(f"❌ Query execution error: {e}")
            print(f"Query: {query}")
            return pd.DataFrame()  # Return empty DataFrame on error
            
        finally:
            # Always return connection to pool
            if connection:
                self.connection_pool.putconn(connection)
    
    def close_all_connections(self):
        """Close all connections in pool"""
        if self.connection_pool:
            self.connection_pool.closeall()
            print("✅ All database connections closed")


# ==================== CONVENIENCE QUERY FUNCTIONS ====================

def execute_query(query, params=None):
    """
    Convenience wrapper for execute_query.
    Uses the singleton connector.
    """
    db = get_db_connector()
    return db.execute_query(query, params)


# ==================== CORE ANALYTICS QUERIES ====================

def fetch_daily_metrics():
    """Fetch latest daily metrics from gold layer"""
    query = """
    SELECT 
        event_date,
        total_events,
        unique_users,
        total_sessions,
        page_views,
        add_to_carts,
        purchases,
        total_revenue,
        avg_order_value,
        cart_rate,
        checkout_rate,
        overall_conversion_rate,
        cart_abandonment_rate,
        revenue_per_user
    FROM gold.mart_daily_metrics
    ORDER BY event_date DESC
    LIMIT 7
    """
    return execute_query(query)


def fetch_conversion_funnel():
    """Fetch conversion funnel data"""
    query = """
    SELECT 
        funnel_stage,
        stage_order,
        users_remaining,
        users_dropped,
        conversion_rate_from_previous,
        conversion_rate_from_start
    FROM gold.mart_conversion_funnel
    ORDER BY stage_order
    """
    return execute_query(query)


def fetch_product_performance(limit=10):
    """Fetch top performing products"""
    query = """
    SELECT 
        product_name,
        product_category,
        total_views,
        total_adds_to_cart,
        total_purchases,
        units_sold,
        total_revenue,
        view_to_cart_rate,
        cart_to_purchase_rate
    FROM gold.mart_product_performance
    ORDER BY total_revenue DESC
    LIMIT %s
    """
    return execute_query(query, (limit,))


def fetch_realtime_events(minutes=5):
    """Fetch events from last N minutes (real-time stream)"""
    query = """
    SELECT 
        event_timestamp,
        event_type,
        product_name,
        product_category,
        country,
        device_type,
        COALESCE(product_price * quantity, 0) as event_value
    FROM silver.stg_clickstream_events
    WHERE event_timestamp >= NOW() - INTERVAL '%s minutes'
    ORDER BY event_timestamp DESC
    LIMIT 100
    """
    return execute_query(query, (minutes,))


def fetch_hourly_trend():
    """Fetch events by hour (today)"""
    query = """
    SELECT 
        event_hour,
        COUNT(*) as event_count,
        COUNT(*) FILTER (WHERE event_type = 'purchase') as purchases,
        SUM(revenue) as hourly_revenue
    FROM silver.stg_clickstream_events
    WHERE event_date = CURRENT_DATE
    GROUP BY event_hour
    ORDER BY event_hour
    """
    return execute_query(query)


def fetch_device_breakdown():
    """Device type distribution"""
    query = """
    SELECT 
        device_type,
        COUNT(*) as sessions,
        SUM(session_revenue) as revenue,
        AVG(session_duration_seconds) as avg_duration
    FROM silver.int_user_sessions
    WHERE DATE(session_start) = CURRENT_DATE
    GROUP BY device_type
    ORDER BY sessions DESC
    """
    return execute_query(query)


# ==================== USER BEHAVIOR QUERIES ====================

def fetch_user_segments():
    """Fetch customer tier distribution"""
    query = """
    SELECT 
        customer_tier,
        COUNT(*) as user_count,
        SUM(lifetime_revenue) as revenue_usd,
        AVG(user_conversion_rate) as avg_conversion,
        AVG(total_sessions) as avg_sessions,
        AVG(purchase_propensity_score) as avg_propensity
    FROM gold.mart_user_behavior
    GROUP BY customer_tier
    ORDER BY 
        CASE customer_tier
            WHEN 'Champion' THEN 1
            WHEN 'Loyal' THEN 2
            WHEN 'Potential' THEN 3
            WHEN 'At Risk' THEN 4
            WHEN 'Lost' THEN 5
        END
    """
    return execute_query(query)


def fetch_lifecycle_stages():
    """Fetch user lifecycle stage breakdown"""
    query = """
    SELECT 
        lifecycle_stage,
        COUNT(*) as user_count,
        AVG(days_since_last_session) as avg_days_inactive,
        SUM(lifetime_revenue) as total_revenue,
        AVG(purchase_propensity_score) as avg_propensity
    FROM gold.mart_user_behavior
    GROUP BY lifecycle_stage
    ORDER BY user_count DESC
    """
    return execute_query(query)


def fetch_engagement_analysis():
    """Engagement level analysis"""
    query = """
    SELECT 
        engagement_level,
        COUNT(*) as users,
        AVG(avg_events_per_session) as avg_events,
        AVG(user_conversion_rate) as avg_conversion,
        COUNT(*) FILTER (WHERE total_purchases > 0) as buyers,
        ROUND(100.0 * COUNT(*) FILTER (WHERE total_purchases > 0) / NULLIF(COUNT(*), 0), 2) as buyer_pct
    FROM gold.mart_user_behavior
    GROUP BY engagement_level
    ORDER BY 
        CASE engagement_level
            WHEN 'Highly Engaged' THEN 1
            WHEN 'Moderately Engaged' THEN 2
            WHEN 'Low Engagement' THEN 3
            WHEN 'Very Low Engagement' THEN 4
        END
    """
    return execute_query(query)


def fetch_rfm_heatmap():
    """RFM score heatmap data"""
    query = """
    SELECT 
        recency_score,
        frequency_score,
        COUNT(*) as user_count,
        AVG(monetary_score) as avg_monetary,
        SUM(lifetime_revenue) as segment_revenue
    FROM gold.mart_user_behavior
    GROUP BY recency_score, frequency_score
    ORDER BY recency_score DESC, frequency_score ASC
    """
    return execute_query(query)


def fetch_hot_leads(limit=10):
    """High-priority cart abandoners"""
    query = """
    SELECT 
        user_id,
        abandoned_sessions,
        days_since_last_session,
        total_add_to_carts,
        highest_order_value,
        purchase_propensity_score,
        preferred_device,
        country
    FROM gold.mart_user_behavior
    WHERE lifecycle_stage = 'Hot Lead (Recent Cart Abandonment)'
    ORDER BY purchase_propensity_score DESC, highest_order_value DESC
    LIMIT %s
    """
    return execute_query(query, (limit,))


def fetch_churned_buyers(limit=10):
    """High-value churned customers"""
    query = """
    SELECT 
        user_id,
        total_purchases,
        lifetime_revenue,
        last_session_date,
        days_since_last_session,
        monetary_segment,
        country
    FROM gold.mart_user_behavior
    WHERE lifecycle_stage = 'Churned Buyer'
        AND lifetime_revenue >= 100
    ORDER BY lifetime_revenue DESC, days_since_last_session ASC
    LIMIT %s
    """
    return execute_query(query, (limit,))


def fetch_top_customers(limit=10):
    """Champion and Loyal customers"""
    query = """
    SELECT 
        user_id,
        customer_tier,
        rfm_segment,
        total_purchases,
        lifetime_revenue,
        total_sessions,
        user_conversion_rate,
        last_session_date,
        country
    FROM gold.mart_user_behavior
    WHERE customer_tier IN ('Champion', 'Loyal')
    ORDER BY lifetime_revenue DESC
    LIMIT %s
    """
    return execute_query(query, (limit,))


def fetch_user_count_by_tier():
    """Quick summary of user counts per tier"""
    query = """
    SELECT 
        customer_tier,
        COUNT(*) as count
    FROM gold.mart_user_behavior
    GROUP BY customer_tier
    """
    return execute_query(query)


def fetch_cohort_analysis():
    """User cohort by first session date"""
    query = """
    SELECT 
        DATE(first_session_date) as cohort_date,
        COUNT(*) as users_acquired,
        COUNT(*) FILTER (WHERE total_purchases > 0) as converted_users,
        ROUND(100.0 * COUNT(*) FILTER (WHERE total_purchases > 0) / COUNT(*), 2) as cohort_conversion_rate,
        AVG(total_sessions) as avg_sessions_per_user,
        SUM(lifetime_revenue) as cohort_revenue
    FROM gold.mart_user_behavior
    GROUP BY DATE(first_session_date)
    ORDER BY cohort_date DESC
    LIMIT 14
    """
    return execute_query(query)


def fetch_device_user_preference():
    """Device preference analysis by customer tier"""
    query = """
    SELECT 
        preferred_device,
        customer_tier,
        COUNT(*) as users,
        AVG(user_conversion_rate) as avg_conversion,
        SUM(lifetime_revenue) as total_revenue
    FROM gold.mart_user_behavior
    GROUP BY preferred_device, customer_tier
    ORDER BY total_revenue DESC
    """
    return execute_query(query)


# ==================== SINGLETON INSTANCE ====================

# Singleton instance
_db_connector = None

def get_db_connector():
    """
    Get or create database connector singleton.
    Ensures only one connection pool exists.
    """
    global _db_connector
    if _db_connector is None:
        _db_connector = DatabaseConnector()
    return _db_connector