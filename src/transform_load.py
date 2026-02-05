import psycopg2
from pymongo import MongoClient
from datetime import datetime
import os
from bs4 import BeautifulSoup
import warnings

# Configuration MongoDB
MONGO_CONFIG = {
    "host": os.getenv("MONGO_HOST", "mongodb"),
    "port": int(os.getenv("MONGO_PORT", 27017)),
    "username": "admin",
    "password": "password",
    "authSource": "admin"
}

# Configuration PostgreSQL
PG_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "dbname": os.getenv("POSTGRES_DB", "airflow")
}

def get_mongo_client():
    return MongoClient(**MONGO_CONFIG)

def get_pg_connection():
    return psycopg2.connect(**PG_CONFIG)

def strip_html(text):
    """Nettoie le HTML des descriptions (EDA Finding 3.1)"""
    if isinstance(text, str) and text:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return BeautifulSoup(text, "html.parser").get_text(separator=' ').strip()
        except Exception:
            return text
    return text

def create_tables():
    """Création des tables dans PostgreSQL (Schéma en étoile)"""
    commands = [
        """
        CREATE TABLE IF NOT EXISTS dim_lieu (
            id SERIAL PRIMARY KEY,
            nom_lieu VARCHAR(255),
            adresse VARCHAR(255),
            code_postal VARCHAR(10),
            ville VARCHAR(100),
            lat FLOAT,
            lon FLOAT,
            UNIQUE(nom_lieu, adresse)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id DATE PRIMARY KEY,
            annee INT,
            mois INT,
            jour INT,
            jour_semaine VARCHAR(20),
            trimestre INT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_categorie (
            id SERIAL PRIMARY KEY,
            categorie VARCHAR(100) UNIQUE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fait_evenement (
            id VARCHAR(255) PRIMARY KEY,
            titre TEXT,
            description TEXT,
            date_debut DATE,
            date_fin DATE,
            url TEXT,
            image_url TEXT,
            prix_detail TEXT,
            lieu_id INT REFERENCES dim_lieu(id),
            categorie_id INT REFERENCES dim_categorie(id),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    ]

    conn = get_pg_connection()
    cur = conn.cursor()

    try:
        print("Création des tables en cours...")
        for command in commands:
            cur.execute(command)
        conn.commit()
        print("Tables créées avec succès.")
    except Exception as e:
        print(f"Erreur lors de la création des tables : {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def extract_from_mongo():
    """Récupère les données brutes depuis MongoDB"""
    client = get_mongo_client()
    db = client['events_db']
    collection = db['raw_events']

    # On récupère les documents
    data = list(collection.find({}))
    client.close()
    return data

def transform_and_load(data):
    """Transforme les données et les charge dans PostgreSQL"""
    conn = get_pg_connection()
    cur = conn.cursor()

    # Dédoublonnage préalable (EDA Finding 3.4)
    # On utilise un dictionnaire pour garder uniquement le dernier événement par ID
    unique_events = {}
    for event in data:
        fields = event.get('fields') or event
        event_id = fields.get('id') or event.get('recordid') or event.get('id')
        if event_id:
            unique_events[event_id] = event
    
    deduplicated_data = list(unique_events.values())
    print(f"Début de la transformation. Événements bruts : {len(data)} -> Après dédoublonnage : {len(deduplicated_data)}")

    try:
        for event in deduplicated_data:
            # Extraction des champs utiles
            fields = event.get('fields')
            if not fields:
                fields = event

            # ID de l'événement (obligatoire)
            event_id = fields.get('id') or event.get('recordid') or event.get('id')
            
            # 1. Gestion de la Dimension Lieu
            nom_lieu = fields.get('nom_de_lieu') or fields.get('lieu_nom') or fields.get('address_name')
            adresse = fields.get('adresse_de_lieu') or fields.get('lieu_adresse') or fields.get('address_street')
            code_postal = fields.get('code_postal') or fields.get('lieu_cp') or fields.get('address_zipcode')
            ville = fields.get('ville') or fields.get('lieu_ville') or fields.get('address_city')

            lat, lon = None, None
            geo = fields.get('lat_lon') or fields.get('geo_point_2d')
            if geo and isinstance(geo, dict):
                lat = geo.get('lat')
                lon = geo.get('lon')
            elif geo and isinstance(geo, list) and len(geo) == 2:
                lat = geo[0]
                lon = geo[1]

            lieu_id = None
            if nom_lieu:
                # Truncate and handle None
                nom_lieu = nom_lieu[:255]
                adresse = (adresse or "")[:255]
                code_postal = (code_postal or "")[:10]
                ville = (ville or "")[:100]

                # On essaie d'insérer, si conflit on update (pour récupérer l'id via RETURNING ou SELECT)
                cur.execute("""
                    INSERT INTO dim_lieu (nom_lieu, adresse, code_postal, ville, lat, lon)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (nom_lieu, adresse) DO UPDATE SET nom_lieu = EXCLUDED.nom_lieu
                    RETURNING id;
                """, (nom_lieu, adresse, code_postal, ville, lat, lon))
                result = cur.fetchone()
                if result:
                    lieu_id = result[0]

            # 2. Gestion de la Dimension Catégorie
            categorie_raw = fields.get('category') or fields.get('categorie')
            categorie_id = None

            if categorie_raw:
                cat_name = categorie_raw[0] if isinstance(categorie_raw, list) and categorie_raw else str(categorie_raw)
                cat_name = cat_name[:100]
                cur.execute("""
                    INSERT INTO dim_categorie (categorie)
                    VALUES (%s)
                    ON CONFLICT (categorie) DO UPDATE SET categorie = EXCLUDED.categorie
                    RETURNING id;
                """, (cat_name,))
                result = cur.fetchone()
                if result:
                    categorie_id = result[0]

            # 3. Gestion de la Dimension Date (Date de début)
            date_debut_str = fields.get('date_start') or fields.get('date_debut')
            date_debut = None
            if date_debut_str:
                try:
                    if date_debut_str.endswith('Z'):
                        date_debut_str = date_debut_str[:-1] + '+00:00'
                    date_debut = datetime.fromisoformat(date_debut_str).date()

                    cur.execute("""
                        INSERT INTO dim_date (date_id, annee, mois, jour, jour_semaine, trimestre)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date_id) DO NOTHING;
                    """, (
                        date_debut,
                        date_debut.year,
                        date_debut.month,
                        date_debut.day,
                        date_debut.strftime('%A'),
                        (date_debut.month - 1) // 3 + 1
                    ))
                except ValueError:
                    pass

            date_fin_str = fields.get('date_end') or fields.get('date_fin')
            date_fin = None
            if date_fin_str:
                try:
                    if date_fin_str.endswith('Z'):
                        date_fin_str = date_fin_str[:-1] + '+00:00'
                    date_fin = datetime.fromisoformat(date_fin_str).date()
                except ValueError:
                    pass

            # 4. Insertion dans la Table de Fait
            titre = fields.get('title') or fields.get('titre')
            
            # Nettoyage HTML de la description (EDA Finding 3.1)
            description_raw = fields.get('description') or fields.get('lead_text')
            description = strip_html(description_raw)
            
            url = fields.get('url') or fields.get('link') or fields.get('contact_url')
            image_url = fields.get('cover_url') or fields.get('image_cover')
            prix_detail = fields.get('price_detail') or fields.get('prix')

            cur.execute("""
                INSERT INTO fait_evenement (id, titre, description, date_debut, date_fin, url, image_url, prix_detail, lieu_id, categorie_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    titre = EXCLUDED.titre,
                    description = EXCLUDED.description,
                    updated_at = CURRENT_TIMESTAMP;
            """, (event_id, titre, description, date_debut, date_fin, url, image_url, prix_detail, lieu_id, categorie_id))

        conn.commit()
        print(f"Transformation et chargement terminés.")

    except Exception as e:
        print(f"Erreur lors du chargement : {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    print("Démarrage du processus ETL...")
    create_tables()
    raw_data = extract_from_mongo()
    if raw_data:
        transform_and_load(raw_data)
    else:
        print("Aucune donnée trouvée dans MongoDB.")