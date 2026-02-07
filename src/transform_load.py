import psycopg2
from pymongo import MongoClient
from datetime import datetime
import os
from bs4 import BeautifulSoup
import warnings

# Configuration MongoDB et Postgres via Variables d'Environnement
MONGO_CONFIG = {
    "host": os.getenv("MONGO_HOST", "mongodb"),
    "port": int(os.getenv("MONGO_PORT", 27017)),
    "username": os.getenv("MONGO_USER", "admin"),
    "password": os.getenv("MONGO_PASSWORD", "password"),
    "authSource": "admin"
}

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

def extract_from_mongo_generator():
    """Récupère les données brutes depuis MongoDB via un curseur (Lazy loading)"""
    client = get_mongo_client()
    db = client['events_db']
    collection = db['raw_events']
    
    # Retourne un curseur au lieu d'une liste
    cursor = collection.find({})
    return cursor, client

def transform_and_load(cursor, client):
    """Transforme les données et les charge dans PostgreSQL par lots"""
    cur = None
    conn = None
    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        
        print(f"--- DÉBUT PROCESSUS ---")
        
        count = 0
        
        # Iteration directe sur le curseur MongoDB (Streaming, mémoire constante)
        for event in cursor:
            count += 1
            if count % 1000 == 0:
                print(f"Traitement de l'événement #{count}...")
            
            # Récupération sécurisée des champs
            # L'API v2.1 retourne une structure plate, v1 utilise 'fields'.
            # On considère 'event' comme la source principale, et 'fields' comme fallback ou structure imbriquée.
            fields = event.get('fields')
            if not fields:
                fields = event 
            
            # ID de l'événement
            event_id = event.get('id') or fields.get('id') or event.get('recordid')
            if not event_id:
                continue
                
            event_id = str(event_id)
            
            # --- 1. Dimension Lieu ---
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
                # Truncate string fields
                nom_lieu = (nom_lieu)[:255]
                adresse = (adresse or "")[:255]
                code_postal = (code_postal or "")[:10]
                ville = (ville or "")[:100]

                cur.execute("""
                    INSERT INTO dim_lieu (nom_lieu, adresse, code_postal, ville, lat, lon)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (nom_lieu, adresse) DO UPDATE SET nom_lieu = EXCLUDED.nom_lieu
                    RETURNING id;
                """, (nom_lieu, adresse, code_postal, ville, lat, lon))
                
                # Fetch result safely
                result = cur.fetchone()
                if result:
                    lieu_id = result[0]
                else:
                    # Dans le cas rare où aucun id n'est retourné (ex: concurrence), on fait un select
                     cur.execute("SELECT id FROM dim_lieu WHERE nom_lieu = %s AND adresse = %s", (nom_lieu, adresse))
                     res = cur.fetchone()
                     if res: lieu_id = res[0]


            # --- 2. Dimension Catégorie ---
            categorie_raw = fields.get('category') or fields.get('categorie') or fields.get('qfap_tags')
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
                else:
                    cur.execute("SELECT id FROM dim_categorie WHERE categorie = %s", (cat_name,))
                    res = cur.fetchone()
                    if res: categorie_id = res[0]

            # --- 3. Dimension Date ---
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

            # --- 4. Table de Fait ---
            titre = fields.get('title') or fields.get('titre')
            
            # Nettoyage HTML
            description_raw = fields.get('description') or fields.get('lead_text')
            description = strip_html(description_raw)
            
            url = fields.get('url') or fields.get('link') or fields.get('contact_url')
            image_url = fields.get('cover_url') or fields.get('image_cover')
            prix_detail = fields.get('price_detail') or fields.get('prix')
            type_prix = fields.get('price_type') or fields.get('access_type')

            # Upsert into Fact Table
            cur.execute("""
                INSERT INTO fait_evenement (id, titre, description, date_debut, date_fin, url, image_url, prix_detail, lieu_id, categorie_id, type_prix)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    titre = EXCLUDED.titre,
                    description = EXCLUDED.description,
                    type_prix = EXCLUDED.type_prix,
                    updated_at = CURRENT_TIMESTAMP;
            """, (event_id, titre, description, date_debut, date_fin, url, image_url, prix_detail, lieu_id, categorie_id, type_prix))

        conn.commit()
        print(f"Transformation et chargement terminés. {count} événements traités.")

    except Exception as e:
        print(f"Erreur lors du chargement : {e}")
        if conn:
            conn.rollback()
    finally:
        if cur: cur.close()
        if conn: conn.close()
        if client: client.close()

def main():
    try:
        cursor, client = extract_from_mongo_generator()
        transform_and_load(cursor, client)
    except Exception as e:
        print(f"Critical error in ETL process: {e}")

if __name__ == "__main__":
    main()
