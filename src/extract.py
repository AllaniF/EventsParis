import requests
import time
import os
import logging
from typing import List, Dict, Any, Optional
from pymongo import MongoClient, UpdateOne
from datetime import datetime

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration variables
MONGO_HOST = os.getenv("MONGO_HOST", "mongodb")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_USER = os.getenv("MONGO_USER", "admin")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD", "password")
MONGO_DB = os.getenv("MONGO_DB", "events_db")

# Constants
API_BASE_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/que-faire-a-paris-/records"
API_TIMEOUT = 20
PAGE_LIMIT = 100

def get_mongo_client() -> MongoClient:
    """
    Configuration de la connexion MongoDB
    """
    return MongoClient(
        host=MONGO_HOST,
        port=MONGO_PORT,
        username=MONGO_USER,
        password=MONGO_PASSWORD,
        authSource="admin"
    )

def save_to_mongo(data: List[Dict[str, Any]], client: MongoClient) -> None:
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
            logger.info(f"Batch processed: {result.upserted_count} inserts, {result.modified_count} updates")
        except Exception as e:
            logger.error("Error executing bulk write", exc_info=True)

def fetch_all_events() -> None:
    """
    Récupère tous les événements via l'API OpenData avec pagination et les sauvegarde.
    """
    offset = 0
    total_events = 0
    
    client = get_mongo_client()

    try:
        while True:
            params = {'limit': PAGE_LIMIT, 'offset': offset}

            try:
                logger.info(f"Fetching records... Offset: {offset}")
                response = requests.get(API_BASE_URL, params=params, timeout=API_TIMEOUT)
                response.raise_for_status()
                
                # Parsing de la réponse JSON
                data = response.json()
                results = data.get('results', [])
                
                # Arrêt de la boucle si aucune donnée n'est trouvée
                if not results:
                    logger.info("No more results found.")
                    break
                
                # Sauvegarde immédiate du lot
                save_to_mongo(results, client)
                total_events += len(results)
                
                # Incrémentation de l'offset pour la page suivante
                offset += PAGE_LIMIT
                
                # Petite pause pour respecter les limites de l'API
                time.sleep(0.2)
                
            except Exception as e:
                logger.error("Error during extraction", exc_info=True)
                break
    finally:
        client.close()

    logger.info(f"Total events fetched and processed: {total_events}")

if __name__ == "__main__":
    # Exécution du processus ETL
    fetch_all_events()
