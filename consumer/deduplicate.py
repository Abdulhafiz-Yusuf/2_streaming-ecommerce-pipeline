# cleanup_duplicates.py
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def cleanup_duplicates():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "streaming_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres123")
    )
    
    cursor = conn.cursor()
    
    # Delete duplicates (keep the latest record)
    cleanup_sql = """
    DELETE FROM bronze.clickstream_events a
    USING bronze.clickstream_events b
    WHERE a.event_id = b.event_id 
      AND a.ctid < b.ctid;
    """
    
    # Count duplicates before cleanup
    count_sql = """
    SELECT event_id, COUNT(*)
    FROM bronze.clickstream_events
    GROUP BY event_id
    HAVING COUNT(*) > 1;
    """
    
    try:
        cursor.execute(count_sql)
        duplicates = cursor.fetchall()
        print(f"Found {len(duplicates)} duplicate event_ids")
        
        cursor.execute(cleanup_sql)
        conn.commit()
        print(f"Deleted duplicate rows")
        
        # Verify cleanup
        cursor.execute(count_sql)
        remaining = cursor.fetchall()
        print(f"Remaining duplicates: {len(remaining)}")
        
    except Exception as e:
        print(f"Error during cleanup: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    cleanup_duplicates()