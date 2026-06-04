# helpers BI
import datetime

import streamlit as st
import pandas as pd
import numpy as np
from zoneinfo import ZoneInfo
from config.towns import FRENCH_TOWNS

import sys
from pathlib import Path
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# markdown
def inject_css() : 
    css = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    html, body, [class*="css"]  { font-family: 'Inter', sans-serif; }
    .main-header {
        font-size: 1.9rem; font-weight: 700; color: #1a1a2e;
        border-left: 5px solid #4361ee; padding-left: 14px;
        margin-bottom: 0.5rem;
    }
    .sub-header { color: #555; font-size: 0.95rem; margin-bottom: 1.5rem; }
    .kpi-box {
        background: linear-gradient(135deg, #4361ee, #3a0ca3);
        border-radius: 14px; padding: 1rem 1.2rem; color: white; text-align: center;
    }
    .kpi-box-green  { background: linear-gradient(135deg, #06d6a0, #028090); border-radius: 14px; padding: 1rem 1.2rem; color: white; text-align: center; }
    .kpi-box-orange { background: linear-gradient(135deg, #f77f00, #d62828); border-radius: 14px; padding: 1rem 1.2rem; color: white; text-align: center; }
    .kpi-box-red    { background: linear-gradient(135deg, #d62828, #6a0572); border-radius: 14px; padding: 1rem 1.2rem; color: white; text-align: center; }
    .kpi-val { font-size: 2rem; font-weight: 700; }
    .kpi-lbl { font-size: 0.78rem; opacity: 0.85; margin-top: 2px; }
    .advisory-ok     { background:#d4edda; border-left:5px solid #28a745; border-radius:10px; padding:1rem; color:#155724; margin:0.8rem 0; }
    .advisory-warn   { background:#fff3cd; border-left:5px solid #ffc107; border-radius:10px; padding:1rem; color:#856404; margin:0.8rem 0; }
    .advisory-danger { background:#f8d7da; border-left:5px solid #dc3545; border-radius:10px; padding:1rem; color:#721c24; margin:0.8rem 0; }
    .alert-row { border-radius:10px; padding:0.7rem 1rem; margin:0.4rem 0; display:flex; align-items:center; gap:12px; }
    .alert-critical { background:#fde8e8; border-left:4px solid #dc3545; }
    .alert-high     { background:#fff4e6; border-left:4px solid #f77f00; }
    .corr-bar { height:8px; border-radius:4px; margin-top:4px; }
</style>
"""
    return css 


def get_aqi_color(aqi):
    if pd.isna(aqi) or aqi == "N/A" : 
        return "#BEBABA"
    if aqi <= 50:  return "#00c853"
    if aqi <= 100: return "#f9a825"
    if aqi <= 150: return "#ef6c00"
    if aqi <= 200: return "#e63131"
    if aqi <= 300: return "#6a1b9a"
    return "#6f0909"

def get_aqi_label(aqi):
    if pd.isna(aqi) or aqi == "N/A": 
        return "N/A"
    else : 
        if aqi <= 50:  return "✅ Bon"
        if aqi <= 100: return "⚠️ Modéré"
        if aqi <= 150: return "🟠 Mauvais (sensibles)"
        if aqi <= 200: return "🔴 Mauvais"
        if aqi <= 300: return "🟣 Très Mauvais"
        return "⚫ Dangereux"

def get_temp_color(temp):
    if pd.isna(temp) or temp == "N/A" : 
        return "#BEBABA"
    if temp <= -15:  return "#4A148C"
    if temp <= -5: return "#1565C0"
    if temp <= 0: return "#42A5F5"
    if temp <= 7: return "#81C784"
    if temp <= 14: return "#CDDC39"
    if temp <= 20: return "#FFEB3B"
    if temp <= 25: return "#FBC02D"
    if temp <= 30: return "#FB8C00"
    if temp <= 35: return "#E53935"
    if temp <= 40: return "#B71C1C"
    return "#880E4F"

def trend_icon(t):
    return {"rising": "📈 Hausse", "falling": "📉 Baisse", "stable": "➡️ Stable"}.get(t, "➡️")

# page kpi : advise
def advisory_html(risk):
    if risk > 50 and risk <=150:
        return '<div class="advisory-warn">⚠️ <b>Précaution</b> — Sensibles (enfants, asthmatiques, personnes âgées) : limitez l\'exposition prolongée.</div>'
    if risk > 150 and risk <= 300:
         return '<div class="advisory-danger">🔴 <b>Alerte Santé</b> — Activités extérieures fortement déconseillées. Groupes sensibles : restez à l\'intérieur.</div>'
    if risk > 300 : 
        return '<div class="advisory-danger">☢️ <b>Danger sanitaire</b> — Restez en intérieur .</div>'
    return '<div class="advisory-ok">✅ <b>Conditions Satisfaisantes</b> — Qualité de l\'air acceptable. Activités normales possibles.</div>'


def weather_condition_details(
    feels_like,
    humidity,
    wind_speed,
    wind_gust=None,
    uvi=None
):
    severity_score = 0
    details = [] 
    
    if feels_like >= 45 or feels_like <= -25:
        severity_score += 4
        details.append("température dangeureuse")
    elif feels_like >= 38 or feels_like <= -15:
        severity_score += 3
        details.append("température extrême")
    elif feels_like >= 30 or feels_like <= -5:
        severity_score += 2
        details.append("température inconfortable")
    elif feels_like >= 27 or feels_like <= 5:
        severity_score += 1
        details.append("température légèrement inconfortable")
        
     # Very humid
    if humidity >= 85:
        severity_score += 2
        details.append("très forte humidité")
    elif humidity >= 70:
        severity_score += 1
        details.append("forte humidité")

    # Very dry
    elif humidity <= 15:
        severity_score += 2
        details.append("air très sec")
    elif humidity <= 25:
        severity_score += 1
        details.append("air sec")
    
    max_wind = wind_gust if wind_gust else wind_speed
    if max_wind >= 30:
        severity_score += 3
        details.append("vent violent")
    elif max_wind >= 20:
        severity_score += 2
        details.append("fort vent")
    elif max_wind >= 12:
        severity_score += 1
        details.append("venteux")
    
    if uvi is not None:
        if uvi >= 11:
            severity_score += 2
            details.append("UV extreme")
        elif uvi >= 8:
            severity_score += 1
            details.append("UV très élevé")
    
    if severity_score >= 9:
        level = "extreme"

    elif severity_score >= 6:
        level = "severe"

    elif severity_score >= 3:
        level = "moderate"

    else:
        level = "comfortable"

    return {
        "severity_score": severity_score,
        "level": level,
        "details": details,
    }

def uvi_color(val):
                if pd.isna(val):  return "#BEBABA"
                if val < 3:       return "#00c853"   # low
                if val < 6:       return "#f9a825"   # moderate
                if val < 8:       return "#ef6c00"   # high
                if val < 11:      return "#c62828"   # very high
                return "#6a0572"                     # extreme

def EN_to_FR() :
     dicoEN_FR = {"thunderstorm" : "Orage",
        "drizzle" : "Bruine",
        "rain" : "Pluie",
        "snow" : "Neige",
        "mist" : "Brume",
        "smoke" : "Fumée",
        "haze" : "Brume",
        "dust" : "Poussière",
        "fog" : "Brouillard",
        "clouds" : "Nuageux",
        "clear" : "Dégagé",
        "safe" : "sûre",
        "caution" : "Inconfort",
        "extreme caution" : "Extrême inconfort",
        "extreme danger" : "Danger extrême",
        "low" : "bas",
        "moderate" : "modéré",
        "high" : "haut",
        "very_high" : "très haut",
        "good" : "bon"
        }
     return dicoEN_FR


def city_infos() : 
    CITIES_COORDS = {
    town.name: {"lat": town.lat, "lon": town.lon}
    for town in FRENCH_TOWNS
    }
    return CITIES_COORDS

# Helpers (handles NA)
def g(r, col, default=None):
    val = r.get(col, default)
    return default if (val is None or (isinstance(val, float) and pd.isna(val))) else val

def fmt(val, spec, fallback="N/A"):
    return format(val, spec) if val is not None else fallback

# container KPI
def kpi(col, val, label,val_min = None,val_max=None, bg_color="#6f83f2"):
    
    minmax_html = ""
    if val_min is not None or val_max is not None:
        minmax_html = f"""
        <div class="kpi-minmax">
            <span class="kpi-min">min {val_min} |</span>
            <span class="kpi-max">max {val_max}</span>
        </div>
        """
    
    col.markdown(
    f'<div class="kpi-box" style="background: linear-gradient(135deg, {bg_color}cc, {bg_color});">'
    f'<div class="kpi-lbl">{label}</div>'
    f'<div class="kpi-val">{val}</div>'
    f'{minmax_html}'
    f'</div>',
    unsafe_allow_html=True
)
    
# Default Session state for selectors
def init_session_defaults():
    defaults = {
        "selected_city": "toulouse",
        "selected_date": datetime.date.today(),
        "selected_range": (datetime.date.today(), datetime.date(2026, 12, 31)),
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val
            
def render_sidebar() :
    init_session_defaults()
    CITIES_COORDS = city_infos()
    st.sidebar.selectbox("Ville",sorted(CITIES_COORDS.keys()), key = "selected_city")
    st.sidebar.date_input("Date d'aujourd'hui", value = 'today',format = "DD.MM.YYYY", key="selected_date")
    st.sidebar.date_input("Période",value=["today", datetime.date(2026, 12, 31)],format = "DD/MM/YYYY", key = "selected_range")