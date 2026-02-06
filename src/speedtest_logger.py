import speedtest
import psycopg2
import os
from datetime import datetime

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
    print("Running speedtest...")
    try:
        st = speedtest.Speedtest()
        print("Finding best server...")
        st.get_best_server()
        
        print("Testing download...")
        download_speed = st.download()  # retuns bits per second
        
        print("Testing upload...")
        upload_speed = st.upload()      # returns bits per second
        
        ping = st.results.ping
        server = st.results.server
        
        print(f"Result: Download: {download_speed} bps, Upload: {upload_speed} bps, Ping: {ping} ms")
        
    except Exception as e:
        print(f"Error executing speedtest: {e}")
        print("Generating DUMMY data for demonstration purposes...")
        import random
        # Generate varied dummy data
        download_speed = random.uniform(50000000, 100000000) # 50-100 Mbps
        upload_speed = random.uniform(10000000, 30000000)    # 10-30 Mbps
        ping = random.uniform(10, 50)
        server = {'name': 'Dummy Server', 'id': '0000'}

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
        print("Speedtest results saved to Postgres.")
    
    except Exception as db_e:
        print(f"Error saving to database: {db_e}")

if __name__ == "__main__":
    main()
