-- Creation des tables pour le Data Warehouse (Couche Silver/Gold)

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

CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    annee INT,
    mois INT,
    jour INT,
    jour_semaine VARCHAR(20),
    trimestre INT
);

CREATE TABLE IF NOT EXISTS dim_categorie (
    id SERIAL PRIMARY KEY,
    categorie VARCHAR(100) UNIQUE
);

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
    type_prix VARCHAR(50),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS speedtest_results (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    download FLOAT,
    upload FLOAT,
    ping FLOAT,
    server_name VARCHAR(255),
    server_id INT
);
