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
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres123")
POSTGRES_DB = os.getenv("POSTGRES_DB", "streaming_db")


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