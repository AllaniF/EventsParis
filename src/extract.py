import requests
import time
from pymongo import MongoClient, UpdateOne
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
    
    # Création d'un index unique sur 'id' pour garantir la performance des upserts
    collection.create_index("id", unique=True)

    operations = []
    # Ajout de la date d'extraction pour le suivi
    for item in data:
        item['extracted_at'] = datetime.utcnow()
        
        # On utilise 'id' (API v2.1) ou 'recordid' (API v2) comme clé unique
        event_id = item.get('id') or item.get('recordid')
        
        if event_id:
            # Upsert: Si l'ID existe, on met à jour, sinon on insère
            operations.append(
                UpdateOne({'id': event_id}, {'$set': item}, upsert=True)
            )
    
    # Exécution en lot (Bulk Write)
    if operations:
        result = collection.bulk_write(operations)
        print(f"--- Processus terminé : {len(operations)} traités. Insérés : {result.upserted_count}, Modifiés : {result.modified_count} ---")
    
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