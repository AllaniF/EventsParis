-- Creation des tables pour le Data Warehouse (Couche Silver/Gold)

CREATE TABLE IF NOT EXISTS dim_events (
    event_id VARCHAR(255) PRIMARY KEY, -- ID original de l'événement
    title VARCHAR(500) NOT NULL,
    description TEXT,
    price_type VARCHAR(50), -- CHECK (price_type IN ('gratuit', 'payant', 'libre')), desactivé pour flexibilité
    access_link VARCHAR(500),
    contact_url VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_dates (
    date_id SERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_name VARCHAR(20),
    month_name VARCHAR(20),
    year INT,
    is_weekend BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_locations (
    location_id SERIAL PRIMARY KEY,
    venue_name VARCHAR(255),
    address_street VARCHAR(500),
    address_zipcode VARCHAR(10),
    address_city VARCHAR(100),
    latitude FLOAT,
    longitude FLOAT
);

CREATE TABLE IF NOT EXISTS fact_event_occurrences (
    occurrence_id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) REFERENCES dim_events(event_id),
    location_id INT REFERENCES dim_locations(location_id),
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    duration_hours FLOAT
);
