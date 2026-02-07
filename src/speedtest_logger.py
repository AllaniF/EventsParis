import speedtest
import psycopg2
import os
import logging
from datetime import datetime

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration PostgreSQL
PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "dbname": os.getenv("POSTGRES_DB", "airflow")
}

def get_pg_connection():
    return psycopg2.connect(**PG_CONFIG)

def main():
    logger.info("Running speedtest...")
    try:
        st = speedtest.Speedtest()
        logger.info("Finding best server...")
        st.get_best_server()
        
        logger.info("Testing download...")
        download_speed = st.download()  # returns bits per second
        
        logger.info("Testing upload...")
        upload_speed = st.upload()      # returns bits per second
        
        ping = st.results.ping
        server = st.results.server
        
        logger.info(f"Result: Download: {download_speed} bps, Upload: {upload_speed} bps, Ping: {ping} ms")
        
        # Save to DB only if successful
        try:
            conn = get_pg_connection()
            cur = conn.cursor()
            
            cur.execute("""
                INSERT INTO speedtest_results (download, upload, ping, server_name, server_id, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (download_speed, upload_speed, ping, server.get('name', 'Unknown'), server.get('id'), datetime.now()))
            
            conn.commit()
            cur.close()
            conn.close()
            logger.info("Speedtest results saved to Postgres.")
        
        except Exception as db_e:
            logger.error("Error saving to database", exc_info=True)

    except Exception as e:
        logger.error("Error executing speedtest. Skipping DB insertion.", exc_info=True)

if __name__ == "__main__":
    main()
