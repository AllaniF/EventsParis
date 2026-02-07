import os
import logging
from pymongo import MongoClient
from typing import Optional

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_mongo_client() -> Optional[MongoClient]:
    """Établit la connexion à MongoDB."""
    host = os.getenv("MONGO_HOST", "mongodb")
    port = int(os.getenv("MONGO_PORT", 27017))
    username = os.getenv("MONGO_USER", "admin")
    password = os.getenv("MONGO_PASSWORD", "password")

    try:
        client = MongoClient(
            host=host,
            port=port,
            username=username,
            password=password,
            authSource="admin",
            serverSelectionTimeoutMS=5000 
        )
        # Test the connection
        client.admin.command('ping')
        logger.info(f"Connexion à MongoDB réussie (Host: {host})")
        return client
    except Exception as e:
        logger.error(f"Erreur de connexion à MongoDB (Host: {host}) : {e}", exc_info=True)
        return None

def list_documents(client: MongoClient) -> None:
    """Affiche les documents de la collection 'raw_events'."""
    if not client:
        return

    try:
        db = client['events_db']
        collection = db['raw_events']

        count = collection.count_documents({})
        logger.info(f"Collection '{collection.name}' contient {count} documents")

        if count > 0:
            logger.info("5 premiers documents :")
            for doc in collection.find().limit(5):
                logger.info(doc)
        else:
            logger.info("La collection est vide.")

    except Exception as e:
        logger.error(f"Erreur lors de la lecture des documents", exc_info=True)
    finally:
        client.close()
        logger.info("Connexion MongoDB fermée.")

if __name__ == "__main__":
    client = get_mongo_client()
    if client:
        list_documents(client)
