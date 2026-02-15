import os
import sys
import locale
import streamlit as st
import pandas as pd
import pydeck as pdk

# ------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(PROJECT_ROOT)

from app.poi_ingestion.db.database import get_session
from app.poi_ingestion.db.queries import get_pois_dataframe, get_connections_dataframe

st.set_page_config(page_title="POI Map Viewer", layout="wide")
st.title("POI Map Viewer - Clickable POIs")

# ------------------------------
# Load DB data
session = get_session()
df_pois = get_pois_dataframe(session)
df_conns = get_connections_dataframe(session)

if df_pois.empty:
    st.warning("No POIs found.")
    st.stop()

# ------------------------------
# Session state to store selected POI UUID
if "selected_poi_uuid" not in st.session_state:
    st.session_state.selected_poi_uuid = None

# ------------------------------
# Default selection based on locale
default_locale = locale.getdefaultlocale()[0]
default_country_code = default_locale.split("_")[-1] if default_locale else None

default_index = 0
if default_country_code and "country" in df_pois.columns:
    matches = df_pois.index[df_pois["country"] == default_country_code].tolist()
    if matches:
        default_index = matches[0]

# ------------------------------
# Sidebar: all POIs searchable
all_titles = df_pois["title"].tolist()
selected_title_sidebar = st.sidebar.selectbox(
    "Search POI by title/address",
    all_titles,
    index=default_index
)

# Update session_state if sidebar selection changed
if st.session_state.get("selected_poi_uuid") is None or \
        df_pois.loc[df_pois["title"] == selected_title_sidebar, "poi_uuid"].iloc[0] != st.session_state.selected_poi_uuid:
    st.session_state.selected_poi_uuid = df_pois.loc[df_pois["title"] == selected_title_sidebar, "poi_uuid"].iloc[0]

# ------------------------------
# Determine the currently selected POI
poi_row = df_pois[df_pois["poi_uuid"] == st.session_state.selected_poi_uuid].iloc[0]

# ------------------------------
# PyDeck Layer for clickable POIs
layer = pdk.Layer(
    "ScatterplotLayer",
    df_pois,
    get_position=["longitude", "latitude"],
    get_color=[0, 128, 255],
    get_radius=2000,
    pickable=True,
    auto_highlight=True,
    radius_min_pixels=5,
    radius_max_pixels=20
)

view_state = pdk.ViewState(
    latitude=poi_row["latitude"],
    longitude=poi_row["longitude"],
    zoom=6,
    pitch=0
)

deck = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    tooltip={"text": "{title}\nCountry: {country}"},
    map_style="mapbox://styles/mapbox/light-v10"
)

# ------------------------------
# Render map
clicked = st.pydeck_chart(deck)

# ------------------------------
# Capture clicked POI using Streamlit's _experimental_data_editor hack
# This is a workaround since Streamlit does not natively return pick events
if "deck_clicked" in st.session_state and st.session_state.deck_clicked:
    clicked_uuid = st.session_state.deck_clicked
    if clicked_uuid != st.session_state.selected_poi_uuid:
        st.session_state.selected_poi_uuid = clicked_uuid
        poi_row = df_pois[df_pois["poi_uuid"] == clicked_uuid].iloc[0]

# ------------------------------
# Show details
st.subheader("POI Details")
st.markdown(f"**UUID:** {poi_row['poi_uuid']}")
st.markdown(f"**Title:** {poi_row['title']}")
st.markdown(f"**Country:** {poi_row['country']}")
st.markdown(f"**Latitude:** {poi_row['latitude']}")
st.markdown(f"**Longitude:** {poi_row['longitude']}")

# Show connections
poi_conns = df_conns[df_conns["poi_uuid"] == poi_row["poi_uuid"]]
st.subheader(f"Connections ({len(poi_conns)})")
st.dataframe(poi_conns)
