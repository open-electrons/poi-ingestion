import os
import sys
import locale
import streamlit as st
import pandas as pd
import pydeck as pdk

# Project path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(PROJECT_ROOT)

from app.poi_ingestion.db.database import get_session
from app.poi_ingestion.db.queries import get_pois_dataframe, get_connections_dataframe

st.set_page_config(page_title="Interactive POI Map", layout="wide")
st.title("Interactive POI Map with Hover-Pick")

# ------------------------------
# Load POIs and connections
session = get_session()
df_pois = get_pois_dataframe(session)
df_conns = get_connections_dataframe(session)

if df_pois.empty:
    st.warning("No POIs found")
    st.stop()

# ------------------------------
# Sidebar: all POIs searchable
all_titles = df_pois["title"].tolist()
default_locale = locale.getdefaultlocale()[0]
default_country_code = default_locale.split("_")[-1] if default_locale else None
default_index = 0
if default_country_code and "country" in df_pois.columns:
    matches = df_pois.index[df_pois["country"] == default_country_code].tolist()
    if matches:
        default_index = matches[0]

# Session state to store selected POI
if "selected_poi_uuid" not in st.session_state:
    st.session_state.selected_poi_uuid = df_pois.iloc[default_index]["poi_uuid"]

sidebar_selection = st.sidebar.selectbox(
    "Search POI by title",
    all_titles,
    index=default_index
)

# Update selection if user uses sidebar
st.session_state.selected_poi_uuid = df_pois[df_pois["title"] == sidebar_selection]["poi_uuid"].iloc[0]

# ------------------------------
# PyDeck layer for clickable/hoverable POIs
layer = pdk.Layer(
    "ScatterplotLayer",
    df_pois,
    get_position=["longitude", "latitude"],
    get_color=[0, 128, 255],
    get_radius=2000,
    pickable=True,
    auto_highlight=True,
    radius_min_pixels=5,
    radius_max_pixels=25
)

# Center map on selected POI
poi_row = df_pois[df_pois["poi_uuid"] == st.session_state.selected_poi_uuid].iloc[0]

view_state = pdk.ViewState(
    latitude=poi_row["latitude"],
    longitude=poi_row["longitude"],
    zoom=8,
    pitch=0
)

deck = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    tooltip={"text": "{title}\nCountry: {country}"},
    map_style="mapbox://styles/mapbox/light-v10"
)

# ------------------------------
# Render map and capture picked object
# NOTE: pydeck does not natively return click events,
# but tooltip hover gives a picked object
picked = st.pydeck_chart(deck)

# ------------------------------
# Details panel updates automatically
poi_row = df_pois[df_pois["poi_uuid"] == st.session_state.selected_poi_uuid].iloc[0]

st.subheader("POI Details")
st.markdown(f"**UUID:** {poi_row['poi_uuid']}")
st.markdown(f"**Title:** {poi_row['title']}")
st.markdown(f"**Country:** {poi_row['country']}")
st.markdown(f"**Coordinates:** {poi_row['latitude']}, {poi_row['longitude']}")

poi_conns = df_conns[df_conns["poi_uuid"] == poi_row["poi_uuid"]]
st.subheader(f"Connections ({len(poi_conns)})")
st.dataframe(poi_conns)
