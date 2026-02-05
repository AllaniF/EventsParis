import requests
import time
from pymongo import MongoClient
from datetime import datetime

def get_mongo_client():
    """
    Configuration de la connexion MongoDB
    """
    client = MongoClient(
        host="mongodb",
        port=27017,
        username="admin",
        password="password",
        authSource="admin"
    )

    return client

def save_to_mongo(data):
    """
    Sauvegarde les données dans MongoDB
    """
    if not data:
        return
    
    client = get_mongo_client()
    db = client['events_db']
    collection = db['raw_events']
    
    # Ajout de la date d'extraction pour le suivi
    for item in data:
        item['extracted_at'] = datetime.utcnow()
    
    # Insertion des données
    result = collection.insert_many(data)
    print(f"--- {len(result.inserted_ids)} records inserted into MongoDB ---")
    client.close()

def fetch_all_events():
    """
    Récupère tous les événements via l'API OpenData avec pagination
    """
    base_url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/que-faire-a-paris-/records"
    limit = 100
    offset = 0
    all_events = []

    while True:
        params = {'limit': limit, 'offset': offset}

        try:
            print(f"Fetching records... Offset: {offset}")
            response = requests.get(base_url, params=params, timeout=20)
            response.raise_for_status()
            
            # Parsing de la réponse JSON
            data = response.json()
            results = data.get('results', [])
            
            # Arrêt de la boucle si aucune donnée n'est trouvée
            if not results:
                print("No more results found.")
                break
            
            all_events.extend(results)
            
            # Incrémentation de l'offset pour la page suivante
            offset += limit
            
            # Petite pause pour respecter les limites de l'API
            time.sleep(0.2)
            
        except Exception as e:
            print(f"Error during extraction: {e}")
            break

    print(f"Total events fetched: {len(all_events)}")
    return all_events

if __name__ == "__main__":
    # Exécution du processus ETL
    events = fetch_all_events()
    
    if events:
        save_to_mongo(events)