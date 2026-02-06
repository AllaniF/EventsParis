import streamlit as st
import pandas as pd
import psycopg2
import os
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Page Layout
st.set_page_config(page_title="Paris Events Statistics", layout="wide")

# Database connection
@st.cache_resource
def get_connection():
    try:
        return psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            user=os.getenv("POSTGRES_USER", "airflow"),
            password=os.getenv("POSTGRES_PASSWORD", "airflow"),
            dbname=os.getenv("POSTGRES_DB", "airflow")
        )
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return None

def load_data(query, conn):
    if conn:
        return pd.read_sql(query, conn)
    return pd.DataFrame()

def main():
    st.title("Paris Events: Operational Analytics")
    st.markdown("### Strategic overview of cultural activity trends and distribution")

    conn = get_connection()
    if not conn:
        return

    # --- 1. Global KPIs ---
    # We execute aggregate queries directly for performance
    kpi_query = """
    SELECT 
        (SELECT COUNT(*) FROM fait_evenement) as total_events,
        (SELECT COUNT(*) FROM dim_lieu) as total_venues,
        (SELECT COUNT(DISTINCT ville) FROM dim_lieu WHERE ville IS NOT NULL) as total_cities,
        (SELECT COUNT(DISTINCT categorie) FROM dim_categorie) as total_categories
    """
    df_kpi = load_data(kpi_query, conn)

    if not df_kpi.empty:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Events", f"{df_kpi['total_events'][0]:,}")
        col2.metric("Venues Referenced", f"{df_kpi['total_venues'][0]:,}")
        col3.metric("Cities Covered", f"{df_kpi['total_cities'][0]}")
        col4.metric("Event Categories", f"{df_kpi['total_categories'][0]}")

    st.markdown("---")

    # --- 2. Temporal Analysis ---
    st.subheader("Temporal Evolution & Seasonality")
    
    query_time = """
        SELECT date_debut 
        FROM fait_evenement 
        WHERE date_debut IS NOT NULL 
        AND date_debut >= '2024-01-01' -- Filter recent relevancy
    """
    df_time = load_data(query_time, conn)

    if not df_time.empty:
        df_time['date_debut'] = pd.to_datetime(df_time['date_debut'])
        
        c1, c2 = st.columns(2)
        
        with c1:
            # Monthly Trend
            df_monthly = df_time.groupby(df_time['date_debut'].dt.to_period('M').astype(str)).size().reset_index(name='count')
            df_monthly.columns = ['Month', 'Event Volume']
            fig_trend = px.area(df_monthly, x='Month', y='Event Volume', title="Monthly Event Volume", markers=True)
            fig_trend.update_layout(xaxis_title=None, showlegend=False)
            st.plotly_chart(fig_trend, use_container_width=True)

        with c2:
            # Day of Week Analysis
            df_time['day_name'] = df_time['date_debut'].dt.day_name()
            day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
            df_dow = df_time['day_name'].value_counts().reindex(day_order).reset_index()
            df_dow.columns = ['Day', 'Events']
            
            fig_dow = px.bar(df_dow, x='Day', y='Events', title="Events by Day of Week", 
                             color='Events', color_continuous_scale='Viridis')
            st.plotly_chart(fig_dow, use_container_width=True)

    # --- 3. Categorical & Geographical Deep Dive ---
    st.subheader("Category & Location Insights")
    
    col_geo, col_cat = st.columns([1, 1])

    with col_geo:
        # Map Density
        query_map = """
            SELECT l.nom_lieu, l.lat, l.lon, COUNT(f.id) as event_count
            FROM fait_evenement f
            JOIN dim_lieu l ON f.lieu_id = l.id
            WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL
            GROUP BY l.nom_lieu, l.lat, l.lon
        """
        df_map = load_data(query_map, conn)
        
        if not df_map.empty:
            # Bubble map
            fig_map = px.scatter_mapbox(
                df_map, 
                lat="lat", lon="lon", 
                size="event_count", 
                color="event_count",
                hover_name="nom_lieu",
                zoom=9, 
                mapbox_style="carto-positron",
                title="Event Density Map (Size = Volume)",
                color_continuous_scale=px.colors.sequential.Plasma
            )
            fig_map.update_layout(margin={"r":0,"t":30,"l":0,"b":0}) # Tight layout
            st.plotly_chart(fig_map, use_container_width=True)

    with col_cat:
        # Top 10 Categories
        query_cat = """
            SELECT c.categorie, COUNT(f.id) as count
            FROM fait_evenement f
            JOIN dim_categorie c ON f.categorie_id = c.id
            GROUP BY c.categorie
            ORDER BY count DESC
            LIMIT 15
        """
        df_cat = load_data(query_cat, conn)
        if not df_cat.empty:
            fig_cat = px.bar(df_cat, x='count', y='categorie', orientation='h', 
                             title="Top 15 Categories by Volume", text_auto=True)
            fig_cat.update_layout(
                yaxis_title=None, 
                xaxis_title=None,
                margin=dict(l=0, r=0, t=30, b=0),
                yaxis={'categoryorder':'total ascending'}
            )
            st.plotly_chart(fig_cat, use_container_width=True)

    # --- 4. Advanced Analytics: Location Focus & Event Duration ---
    st.markdown("---")
    st.subheader("Deep Dive: Paris Arrondissements & Event Types")

    c_city, c_dur = st.columns(2)

    with c_city:
        # Paris Arrondissements Analysis
        query_paris = """
            SELECT l.code_postal, COUNT(f.id) as count
            FROM fait_evenement f
            JOIN dim_lieu l ON f.lieu_id = l.id
            WHERE l.code_postal LIKE '75%'
            GROUP BY l.code_postal
            ORDER BY count DESC
            LIMIT 10
        """
        df_paris = load_data(query_paris, conn)
        
        if not df_paris.empty:
            # Clean up zip codes to just show '1er', '2eme' etc if possible, or just the code
            df_paris['Arrondissement'] = df_paris['code_postal'].apply(lambda x: f"{x[-2:]}e Arr" if x.startswith('75') else x)
            
            fig_paris = px.bar(df_paris, x='Arrondissement', y='count', 
                               title="Top 10 Paris Districts (Arrondissements)",
                               color='count', color_continuous_scale='Reds')
            st.plotly_chart(fig_paris, use_container_width=True)

    with c_dur:
        # Event Duration Analysis
        query_duration = """
            SELECT
                CASE 
                    WHEN date_fin IS NULL OR date_fin = date_debut THEN '1. Single Day'
                    WHEN date_fin - date_debut <= 7 THEN '2. Short Week (2-7 days)'
                    WHEN date_fin - date_debut <= 30 THEN '3. Month-Long (8-30 days)'
                    ELSE '4. Long Term exhibition (>30 days)'
                END as duration_type,
                COUNT(id) as count
            FROM fait_evenement
            WHERE date_debut IS NOT NULL
            GROUP BY 1
            ORDER BY 1
        """
        df_duration = load_data(query_duration, conn)
        
        if not df_duration.empty:
            fig_dur = px.pie(df_duration, values='count', names='duration_type', 
                             title="Event Duration Distribution",
                             hole=0.4,
                             color_discrete_sequence=px.colors.sequential.Teal)
            st.plotly_chart(fig_dur, use_container_width=True)

if __name__ == "__main__":
    main()
