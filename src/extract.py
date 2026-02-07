import requests
import time
import os
from pymongo import MongoClient, UpdateOne
from datetime import datetime

# Configuration variables
MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password")
MONGO_DB = os.getenv("MONGO_DB", "events_db")

def get_mongo_client():
    """
    Configuration de la connexion MongoDB
    """
    client = MongoClient(
        host=MONGO_HOST,
        port=MONGO_PORT,
        username=MONGO_USER,
        password=MONGO_PASSWORD,
        authSource="admin"
    )
    return client

def save_to_mongo(data, client):
    """
    Sauvegarde les données dans MongoDB avec Upsert pour éviter les doublons
    """
    if not data:
        return
    
    db = client[MONGO_DB]
    collection = db['raw_events']
    
    # Préparation des opérations Bulk Upsert
    operations = []
    
    for item in data:
        # Ajout de la date d'extraction
        item['extracted_at'] = datetime.utcnow()
        
        # Identification unique de l'événement
        # On utilise l'ID fourni par l'API s'il existe, sinon on le génère
        event_id = item.get('id') or item.get('recordid')
        
        if event_id:
            operations.append(
                UpdateOne(
                    {'id': event_id},   # Filtre
                    {'$set': item},     # Mise à jour
                    upsert=True         # Insérer si n'existe pas
                )
            )
    
    if operations:
        try:
            result = collection.bulk_write(operations)
            print(f"--- Batch processed: {result.upserted_count} inserts, {result.modified_count} updates ---")
        except Exception as e:
            print(f"Error executing bulk write: {e}")

def fetch_all_events():
    """
    Récupère tous les événements via l'API OpenData avec pagination et les sauvegarde.
    """
    base_url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/que-faire-a-paris-/records"
    limit = 100
    offset = 0
    total_events = 0
    
    client = get_mongo_client()

    try:
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
                save_to_mongo(results, client)
                total_events += len(results)
                
                # Incrémentation de l'offset pour la page suivante
                offset += limit
                
                # Petite pause pour respecter les limites de l'API
                time.sleep(0.2)
                
            except Exception as e:
                print(f"Error during extraction: {e}")
                break
    finally:
        client.close()

    print(f"Total events fetched and processed: {total_events}")

if __name__ == "__main__":
    # Exécution du processus ETL
    fetch_all_events()
