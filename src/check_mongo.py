from pymongo import MongoClient
import os

def get_mongo_client():
    """Établit la connexion à MongoDB."""
    try:
        host = os.getenv("MONGO_HOST", "mongodb")
        port = int(os.getenv("MONGO_PORT", 27017))
        username = os.getenv("MONGO_USER", "admin")
        password = os.getenv("MONGO_PASSWORD", "password")

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
        print(f"--- Connexion à MongoDB réussie (Host: {host}) ---")
        return client
    except Exception as e:
        print(f"Erreur de connexion à MongoDB (Host: {host}) : {e}")
        return None

def list_documents(client):
    """Affiche les documents de la collection 'raw_events'."""
    if not client:
        return

    try:
        db = client['events_db']
        collection = db['raw_events']

        count = collection.count_documents({})
        print(f"\n--- Collection '{collection.name}' contient {count} documents ---")

        if count > 0:
            print("\n--- 5 premiers documents ---")
            for doc in collection.find().limit(5):
                print(doc)
        else:
            print("La collection est vide.")

    except Exception as e:
        print(f"Erreur lors de la lecture des documents : {e}")
    finally:
        client.close()
        print("\n--- Connexion à MongoDB fermée ---")

if __name__ == "__main__":
    client = get_mongo_client()
    list_documents(client)
