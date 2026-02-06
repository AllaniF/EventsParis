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
    
    # Suppression de l'index unique s'il existe pour permettre l'insertion de tous les événements (doublons inclus)
    try:
        collection.drop_index("id_1")
    except Exception:
        pass

    # Ajout de la date d'extraction pour le suivi
    for item in data:
        item['extracted_at'] = datetime.utcnow()
    
    # Insertion directe (Append-only) sans vérification de doublons
    if data:
        collection.insert_many(data)
        print(f"--- Processus terminé : {len(data)} ajoutés. ---")
    
    client.close()

def fetch_all_events():
    """
    Récupère tous les événements via l'API OpenData avec pagination et les sauvegarde.
    """
    base_url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/que-faire-a-paris-/records"
    limit = 100
    offset = 0
    total_events = 0

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
            
            # Sauvegarde immédiate du lot
            save_to_mongo(results)
            total_events += len(results)
            
            # Incrémentation de l'offset pour la page suivante
            offset += limit
            
            # Petite pause pour respecter les limites de l'API
            time.sleep(0.2)
            
        except Exception as e:
            print(f"Error during extraction: {e}")
            break

    print(f"Total events fetched and saved: {total_events}")

if __name__ == "__main__":
    # Exécution du processus ETL
    fetch_all_events()