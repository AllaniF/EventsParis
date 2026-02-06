import streamlit as st
import pandas as pd
import psycopg2
import os
import pydeck as pdk

# Database connection
def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        dbname=os.getenv("POSTGRES_DB", "airflow")
    )

st.set_page_config(page_title="Events Paris Dashboard", layout="wide")
st.title("Events Paris Dashboard")

try:
    conn = get_connection()

    # KPI: Total Events
    query_total = "SELECT COUNT(*) FROM fait_evenement;"
    total_events = pd.read_sql(query_total, conn).iloc[0, 0]

    # KPI: Total Locations
    query_locations = "SELECT COUNT(*) FROM dim_lieu;"
    total_locations = pd.read_sql(query_locations, conn).iloc[0, 0]

    col1, col2 = st.columns(2)
    col1.metric("Total Events", total_events)
    col2.metric("Total Locations", total_locations)

    st.divider()

    col_cat, col_city = st.columns(2)

    with col_cat:
        # Chart: Events by Category
        st.subheader("Events by Category")
        query_category = """
            SELECT c.categorie, COUNT(f.id) as count
            FROM fait_evenement f
            JOIN dim_categorie c ON f.categorie_id = c.id
            GROUP BY c.categorie
            ORDER BY count DESC;
        """
        df_category = pd.read_sql(query_category, conn)
        st.bar_chart(df_category.set_index("categorie"))

    with col_city:
        # Chart: Events by City (Ville)
        st.subheader("Events by City")
        query_city = """
            SELECT l.ville, COUNT(f.id) as count
            FROM fait_evenement f
            JOIN dim_lieu l ON f.lieu_id = l.id
            WHERE l.ville IS NOT NULL
            GROUP BY l.ville
            ORDER BY count DESC;
        """
        df_city = pd.read_sql(query_city, conn)
        st.bar_chart(df_city.set_index("ville"))

    st.divider()

    # Map: Event Locations
    st.subheader("Event Locations Map")
    query_map = """
        SELECT l.lat, l.lon
        FROM fait_evenement f
        JOIN dim_lieu l ON f.lieu_id = l.id
        WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL;
    """
    df_map = pd.read_sql(query_map, conn)
    # Streamlit map expects columns named 'lat' and 'lon' or 'latitude' and 'longitude'
    df_map = df_map.rename(columns={'lat': 'latitude', 'lon': 'longitude'})

    if not df_map.empty:
        st.pydeck_chart(pdk.Deck(
            map_style=None,
            initial_view_state=pdk.ViewState(
                latitude=48.8566,
                longitude=2.3522,
                zoom=9,
                pitch=0,
            ),
            layers=[
                pdk.Layer(
                    'ScatterplotLayer',
                    data=df_map,
                    get_position='[longitude, latitude]',
                    get_color='[200, 30, 0, 160]',
                    get_radius=100,
                ),
            ],
        ))
    else:
        st.write("No location data available for map.")

    conn.close()

except Exception as e:
    st.error(f"Error connecting to database: {e}")
