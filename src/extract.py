import requests
from pymongo import MongoClient
from datetime import datetime

def get_mongo_client():
   
    MongoClient(
        host="mongodb",
        port=27017,
        username="admin",
        password="password",
        authSource="admin"
    )

    return client

def save_to_mongo(data):
    if not data:
        return
    
    client = get_mongo_client()
    db = client['events_db']
    collection = db['raw_events']
    
    # On ajoute une date d'insertion pour le suivi du Datalake
    for item in data:
        item['extracted_at'] = datetime.utcnow()
    
    # Insertion des données (insert_many est plus performant)
    result = collection.insert_many(data)
    print(f"--- {len(result.inserted_ids)} documents insérés dans MongoDB ---")
    client.close()

def fetch_paris_events( offset=0):
    base_url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/que-faire-a-paris-/records"
    params = { 'offset': offset}

    try:
        print(f"Extraction de {limit} événements...")
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        
        results = response.json().get('results', [])
        print(f"Succès : {len(results)} événements récupérés.")
        return results
    except Exception as err:
        print(f"Erreur d'extraction : {err}")
        return []

if __name__ == "__main__":
    # 1. Extraction
    raw_data = fetch_paris_events()
    
    # 2. Stockage dans le Datalake
    if raw_data:
        save_to_mongo(raw_data)