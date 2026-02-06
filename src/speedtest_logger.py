import speedtest
import datetime
import os
import psycopg2
import time
import requests
import re
import subprocess
import sys

def run_fallback_speedtest():
    print("Ejecutando Speedtest FALLBACK (requests)...", flush=True)
    results = {
        "download": 0.0,
        "upload": 0.0,
        "ping": 0.0,
        "share": "fallback",
        "server": {"id": 0, "name": "Fallback", "country": "N/A", "sponsor": "Manual"},
        "timestamp": datetime.datetime.utcnow()
    }
    
    # Ping
    try:
        # Ping google
        cmd = ["ping", "-c", "1", "8.8.8.8"]
        output = subprocess.check_output(cmd).decode().strip()
        # Parse time=X ms
        match = re.search(r'time=([\d.]+)\s*ms', output)
        if match:
            results["ping"] = float(match.group(1))
    except Exception as e:
        print(f"Fallback Ping Error: {e}", flush=True)

    # Download Test (10MB file from Tele2)
    try:
        url = "http://speedtest.tele2.net/10MB.zip"
        start = time.time()
        # Timeout corto para evitar bloqueos
        r = requests.get(url, stream=True, timeout=10)
        total_bytes = 0
        for chunk in r.iter_content(chunk_size=8192):
            total_bytes += len(chunk)
            # Break early if taking too long (e.g. 2MB reached just to prove it works)
            if total_bytes > 1024 * 1024 * 2: 
                break
                
        duration = time.time() - start
        
        # Bits per second
        if duration > 0:
            bps = (total_bytes * 8) / duration
            results["download"] = bps
        
        print(f"Fallback Download Speed: {results['download']/1000000:.2f} Mbps", flush=True)
            
    except Exception as e:
         print(f"Fallback Download Error: {e}", flush=True)

    # Upload Test (Optional/Mock for now)
    results["upload"] = results["download"] * 0.5 

    return results

def run_speedtest():
    print("Ejecutando Speedtest...", flush=True)
    try:
        # secure=True ayuda a evitar problemas de certificados SSL
        s = speedtest.Speedtest(secure=True)
        print("Buscando servidores...", flush=True)
        s.get_servers()
        
        if not s.servers:
             raise Exception("Lista de servidores vacia")

        print("Seleccionando el mejor servidor...", flush=True)
        try:
            s.get_best_server()
        except:
             # Fallback interno de servidor
             first_list = list(s.servers.values())[0]
             s.best.update(first_list[0])

        print("Iniciando descarga...", flush=True)
        s.download()
        print("Iniciando subida...", flush=True)
        s.upload()
        
        res = s.results.dict()
        res['timestamp'] = datetime.datetime.utcnow()
        return res

    except Exception as e:
        print(f"Speedtest-cli falló: {e}. Usando método fallback.", flush=True)
        return run_fallback_speedtest()

def save_to_postgres(data):
    # Valores por defecto para el servicio docker 'postgres'
    pg_host = os.getenv('POSTGRES_HOST', 'postgres')
    # pg_host = 'localhost' # Descomentar para test local fuera de docker
    pg_user = os.getenv('POSTGRES_USER', 'airflow')
    pg_password = os.getenv('POSTGRES_PASSWORD', 'airflow')
    pg_db = os.getenv('POSTGRES_DB', 'airflow')
    
    print(f"Connecting to Postgres at {pg_host}...", flush=True)
    try:
        conn = psycopg2.connect(
            host=pg_host,
            user=pg_user,
            password=pg_password,
            dbname=pg_db
        )
        cursor = conn.cursor()
        
        query = """
            INSERT INTO speedtest_results (download, upload, ping, timestamp, share, server_id, server_name, server_country)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        server = data.get('server', {})
        
        cursor.execute(query, (
            data['download'],
            data['upload'],
            data['ping'],
            data['timestamp'],
            data['share'],
            server.get('id'),
            server.get('name'),
            server.get('country')
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Resultado guardado en Postgres exitosamente.", flush=True)
        
    except Exception as e:
        print(f"Error guardando en Postgres: {e}", flush=True)
        raise e

def main():
    try:
        data = run_speedtest()
        save_to_postgres(data)
    except Exception as e:
        print(f"Error General: {e}", flush=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
