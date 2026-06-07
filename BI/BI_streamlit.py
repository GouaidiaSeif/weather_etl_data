"""V2 modifie la pip d'aggregation de poll et weather"""

# comparer les perf si  filtre tout avec python ou si avec mongo db. (surtout pour les requetes où peut y avoir bcp de données retournée)
# class par page
# class pour load les datas

import streamlit as st
import json
from datetime import datetime,timedelta
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from BI.helpers_BI import city_infos,inject_css,init_session_defaults, render_sidebar

# from BI.page_map import render_map_page
# from BI.Alerte_page import render_alerts_page

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(
    page_title="🌍 AirMonitor France",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)


st.markdown(inject_css(), unsafe_allow_html=True)

CITIES_COORDS = city_infos()
init_session_defaults()
# render_sidebar()
# ============================================================
# SIDE BAR
# ============================================================
# st.set_page_config(
#     page_title="🌍 AirMonitor France",
#     page_icon="🌍",
#     layout="wide",
#     initial_sidebar_state="expanded",
# )



with st.sidebar:
    st.markdown("## Surveillance Qualité de l'Air & Météo")
    
    st.markdown("---")
    city = st.selectbox("Ville",sorted(CITIES_COORDS.keys()), key = "selected_city")
    st.markdown("---")
    today_date = st.date_input("Date", value = 'today',format = "DD/MM/YYYY", key="selected_date")
    st.markdown("---")
    date_range = st.date_input("Période",value=[datetime(2026,6,1), datetime(2026, 12, 31)],format = "DD/MM/YYYY", key = "selected_range")
    
    
   
## mettre des tabs pour météo du jour résumé ou météo par heure? avec changement de la selectbox au changemen de tab
pg = st.navigation([
    st.Page("page_map.py", title="Carte Interactive", icon="🗺️"),
    st.Page("page_KPI.py", title="KPI historiques", icon="📊"),
    st.Page("page_alertes.py", title="Alertes", icon="🚨"),
    st.Page("page_coor.py", title="Corrélations"),
],position="top")
pg.run()


    