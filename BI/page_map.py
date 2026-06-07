"""V2 modifie la pip d'aggregation de poll et weather"""

# comparer les perf si  filtre tout avec python ou si avec mongo db. (surtout pour les requetes où peut y avoir bcp de données retournée)
# class par page
# class pour load les datas
# afficher les données horaires avec ticks tout les 3 ou 6h 


import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config.towns import FRENCH_TOWNS
# from Alerte_page import render_alerts_page
from BI.helpers_BI import city_infos,g,fmt, get_aqi_color,get_aqi_label,get_temp_color, init_session_defaults, render_sidebar
from BI.helpers_BI import inject_css,trend_icon,advisory_html,weather_condition_details,uvi_color,EN_to_FR
from BI.data_loader import load_hourly_data,load_agg_data
from BI.data_gen import fill_df_jour


st.set_page_config(
    page_title="Carte Météo & Qualité de l'Air",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# CSS
# ============================================================
st.markdown(inject_css(), unsafe_allow_html=True)

# init_session_defaults()
# render_sidebar()

CITIES_COORDS = city_infos()
# english to french
dicoEN_FR = EN_to_FR()


st.markdown('<div class="main-header">🗺️ Carte Météo & Qualité de l\'Air</div>', unsafe_allow_html=True)

# sel_date = st.date_input("Sélectionnez une date (aujourd'hui par défaut)", value = 'today'
#                              ,format = "DD-MM-YYYY",key = "data_map")
# sel_date = st.session_state["data_map"]
sel_date = st.session_state["selected_date"]

st.markdown(f'<div class="sub-header">Données du <b>{sel_date}</b>.  Survolez une ville pour tous les détails</div>', unsafe_allow_html=True)


# df_date = load_daily(date=today_date.isoformat())
#données agrégées
df_agg  = load_agg_data(date=sel_date.isoformat())

# charge df avec données horaire
data_heure = load_hourly_data(date = sel_date.isoformat(),city=None)

# Helpers (handles NA)

# Tabs : daily, hourly data
tab_daily, tab_hourly = st.tabs(["📅 Résumé Journée", "🕐 Météo horaire"])


def hover_hourly(r):
            return (
                f"<b>📍 {str.capitalize(g(r, 'city', '?'))}</b> — "
                f"<b>{g(r, 'hour_formatted', '?')}</b><br>"
                f"─────────────────<br>"
                f"🌡️ Temp : <b>{fmt(g(r,'temperature'), '.1f')}°C "
                        f"(ressentie {fmt(g(r,'feels_like'), '.1f')}°C)</b><br>"
                f"💧 Humidité : <b>{fmt(g(r,'humidity'), '.0f')}%</b><br>"
                f"⏲ Pression : <b>{fmt(g(r,'pressure_hpa'), '.0f')}hPa</b><br>"
                f"💨 Vent : <b>{fmt(g(r,'wind_speed'), '.1f')} m/s "
                        f"({g(r,'wind_direction_cardinal','?')})</b><br>"
                f"💨 Rafales : <b>{fmt(g(r,'wind_gust_mps'), '.1f')} m/s</b><br>"
                f"☀️ UVI : <b>{fmt(g(r,'uvi'), '.1f')} ({dicoEN_FR.get(g(r,'uvi_category','?'))})</b><br>"
                f"⛅ Météo : <b>{dicoEN_FR.get((g(r,'weather','?')))}</b><br>"
                f"⚠️ Alerte chaleur : <b>{dicoEN_FR.get(g(r,'heat_index_warning','Aucune'))}</b><br>"
                f"─────────────────<br>"
                f"🏭 AQI : <b>{fmt(g(r,'aqi'), '.0f')}</b> ({get_aqi_label(g(r,'aqi',"N/A"))})<br>"
                f"🔵 PM2.5 : <b>{fmt(g(r,'pm25'), '.1f')}</b><br>"
                f"🟡 PM10 : <b>{fmt(g(r,'pm10'), '.1f')}</b><br>"
                f"🔴 NO₂ : <b>{fmt(g(r,'no2'), '.1f')}</b><br>"
                f"🟢 O₃ : <b>{fmt(g(r,'o3'), '.1f')}</b><br>"
                f"Polluant Dom. : <b>{g(r,'primary_pollutant','N/A')}</b><br>"
                f"─────────────────<br>"
            )

def hover_dayly(r):
                return (
                    f"<b>📍 {str.capitalize(g(r, 'city', '?'))}</b><br>"
                f"─────────────────<br>"
                f"🌡️ Temp : <b>{fmt(g(r,'w_avg_temperature'), '.1f')}°C "
                f"(ressentie {fmt(g(r,'w_avg_feels_like'), '.1f')}°C)</b><br>"
                f"🌡️ Min/Max : <b>{fmt(g(r,'w_min_temperature'), '.1f')}° / {fmt(g(r,'w_max_temperature'), '.1f')}°</b><br> "
                f"💧 Humidité : <b>{fmt(g(r,'w_avg_humidity'), '.0f')}%</b><br>"
                f"⏲ Pression : <b>{fmt(g(r,'w_avg_pressure'), '.0f')}hPa</b><br>"
                f"💨 Vent Max : <b>{fmt(g(r,'w_max_wind_speed'), '.1f')} m/s</b><br>"
                f"🌧️ Précip. : <b>{'Oui' if g(r,'w_precipitation_detected') else 'Non'}</b><br>"
                f"☁️ Couverture Nuageuse : <b>{fmt(g(r,'w_avg_cloud_coverage'), '.1f')}%</b><br>"
                f"☀️ UVI Max : <b>{fmt(g(r,'w_max_uvi'), '.1f')}</b><br>"
                
                f"─────────────────<br>"
                f"🏭 AQI : <b>{fmt(g(r,'pol_avg_aqi'), '.0f')}</b>  ({get_aqi_label(g(r,'pol_avg_aqi'))})<br>"
                f"🔵 PM2.5 : <b>{fmt(g(r,'pol_avg_pm25'), '.1f')}</b><br>"
                f"🟡 PM10 : <b>{fmt(g(r,'pol_avg_pm10'), '.1f')}</b><br>"
                f"🔴 NO₂ : <b>{fmt(g(r,'pol_avg_no2'), '.1f')}</b><br>"
                f"🟢 O₃ : <b>{fmt(g(r,'pol_avg_o3'), '.1f')}</b><br>"
                f"─────────────────<br>"
                f"⛅ Condition météo : <b>{dicoEN_FR.get(g(r,'w_dominant_weather_condition'),"N/A")}</b><br>"
                f"🌡️ Tendance T° : <b>{trend_icon(g(r,'w_temp_trend'))}</b><br>"
                f"📈 Tendance AQI : <b>{trend_icon(g(r,'pol_aqi_trend'))}</b>"
                )

# en argument les df df_agg et data_heure
# def render_map_page(df_agg,data_heure) : 

# ════════════════════════════════════════════════════════════════════════
# TAB 1 — Carte journalière 
# ════════════════════════════════════════════════════════════════════════

map_style = "carto-positron"
with tab_daily:
    col_map, col_ctrl = st.columns([3, 1])

    with col_ctrl:
        # size_by   = st.selectbox("Taille des points", ["AQI", "Température"], key="size_daily")
        color_by  = st.selectbox("Couleur", ["AQI (norme)", "Température"], key="color_daily")
        if color_by == "AQI (norme)":
            st.markdown("##### Échelle AQI")
            for rng, lbl, col in [
                ("0–50",    "Bon",               "#00c853"),
                ("51–100",  "Modéré",            "#f9a825"),
                ("101–150", "Mauvais (sensibles)","#ef6c00"),
                ("151–200", "Mauvais",            "#c62828"),
                ("201-300", "Très mauvais",       "#6a1b9a"),
                (">300",    "Dangereux",          "#6f0909"),
                ("No data","","#BEBABA")
            ]:
                st.markdown(
                    f'<div style="display:flex;align-items:center;gap:8px;margin:3px 0">'
                    f'<div style="width:16px;height:16px;border-radius:50%;background:{col}"></div>'
                    f'<span style="font-size:0.78rem"><b>{rng}</b> {lbl}</span></div>',
                    unsafe_allow_html=True
                )
        else :   
            st.markdown("##### Échelle Témpérature")
            for rng, lbl, col in [
                ("< -15",    "Froid Extrême", "#4A148C"),
                ("-15–-5",  "Grand Froid", "#1565C0"),
                ("-5-0", "Gel","#42A5F5"),
                ("0-7", "Frais",   "#81C784"),
                ("7-14", "Douceur", "#CDDC39"),
                ("14-20", "Agréable", "#FFEB3B"),
                ("20-25", "Chaud", "#FBC02D"),
                ("25-30", "Très Chaud", "#FB8C00"),
                ("30-35", "Forte Chaleur",  "#E53935"),
                (">35",    "Chaleur Extrême", "#B71C1C"),
                ("No data","","#BEBABA")
            ]:
                st.markdown(
                    f'<div style="display:flex;align-items:center;gap:8px;margin:3px 0">'
                    f'<div style="width:16px;height:16px;border-radius:50%;background:{col}"></div>'
                    f'<span style="font-size:0.78rem"><b>{rng}</b> {lbl}</span></div>',
                    unsafe_allow_html=True
                )    
        
    if df_agg.empty:
        st.info(f"Pas de données pour la date {sel_date}")
    else : 
        df_agg["hover"] = df_agg.apply(hover_dayly, axis=1)
                    
        fig_map = go.Figure()
        for _, row in df_agg.iterrows():
            mc = get_aqi_color(row["pol_avg_aqi"]) if color_by == "AQI (norme)" else get_temp_color(row["w_avg_temperature"])
            fig_map.add_trace(go.Scattermapbox(
                lat=[CITIES_COORDS[row["city"]].get("lat")], 
                lon=[CITIES_COORDS[row["city"]].get("lon")],
                mode="markers+text",
                marker=dict(size=25, color=mc, opacity=0.85),
                text=[row["city"]], textposition="top center",
                textfont=dict(size=11, color="#1a1a2e", family="Inter"),
                hovertext=row["hover"], hoverinfo="text",
                showlegend=False,
            ))
            fig_map.add_trace(go.Scattermapbox(
                lat=[CITIES_COORDS[row["city"]].get("lat")], lon=[CITIES_COORDS[row["city"]].get("lon")],
                mode="text",
                text=[f"{row['pol_avg_aqi']:.0f}"],
                textfont=dict(size=10, color="white", family="Inter"),
                hoverinfo="skip", showlegend=False,
            ))

        fig_map.update_layout(
            mapbox=dict(style=map_style, center=dict(lat=46.6, lon=2.5), zoom=4.6),
            margin=dict(l=0, r=0, t=0, b=0), height=560,
        )
        with col_map:
            st.plotly_chart(fig_map, width='stretch')

        st.markdown("---")
        st.markdown(f"### Tableau de synthèse : {sel_date}")
        
        df_show = df_agg[[
            "city","pol_avg_aqi","pol_max_alert_level","pol_aqi_trend",
            "pol_avg_pm25","pol_avg_pm10","pol_avg_no2","pol_avg_o3",
            "w_avg_temperature","w_temp_trend","w_avg_humidity","w_avg_pressure","w_avg_wind_speed","w_dominant_weather_condition"
            ]].copy()
        df_show.columns = ["Ville","AQI","Alerte pollution","Tendance","PM2.5","PM10","NO₂","O3"
                        ,"Temp °C","Tendance T°","Humidité %","Pression","Vent m/s","Condition météo"]
        df_show["Alerte pollution"] = df_show["AQI"].apply(get_aqi_label) 
        df_show["Tendance"] = df_show["Tendance"].map({"rising":"📈 Hausse","falling":"📉 Baisse","stable":"➡️ Stable"})
        df_show["Tendance T°"] = df_show["Tendance T°"].map({"rising":"📈 Hausse","falling":"📉 Baisse","stable":"➡️ Stable"})
        df_show["Condition météo"] = df_show["Condition météo"].map(dicoEN_FR)
        df_show["Alerte météo"] = df_agg.apply(
            lambda row : weather_condition_details(
                feels_like = row["w_avg_feels_like"],
                humidity= row["w_avg_humidity"],
                wind_speed= row["w_avg_wind_speed"],
                wind_gust= row["w_max_wind_gust"],
                uvi = row["w_avg_uvi"],
            )["details"], axis = 1)
        st.dataframe(df_show.set_index("Ville").sort_values("AQI", ascending=False), width='stretch')


# ════════════════════════════════════════════════════════════════════════
# TAB 2 — Carte horaire
# ════════════════════════════════════════════════════════════════════════
with tab_hourly:
    # ── Sélecteur d'heure ───────────────────────────────────────────────
    if data_heure.empty:
        st.info(f"Pas de données pour la date données horaire {sel_date}")
    else : 
        heures_dispo = sorted(data_heure["hour"].dropna().unique().astype(int).tolist())
        heure_labels = {h: data_heure.loc[data_heure["hour"] == h, "hour_formatted"].iloc[0]
                        for h in heures_dispo
                        if not data_heure.loc[data_heure["hour"] == h, "hour_formatted"].empty}

        selected_hour = st.segmented_control(
            "🕐 Heure locale",
            options=heure_labels,
            format_func=lambda h: heure_labels.get(h),
            default=heures_dispo[0] if heures_dispo else None,
            key="hour_ctrl"
        )

        if selected_hour is None:
            st.info("Sélectionnez une heure pour afficher la carte.")
            st.stop()

        df_h = data_heure[data_heure["hour"] == selected_hour].copy()
        col_map_h, col_ctrl_h = st.columns([3, 1])

        with col_ctrl_h:
            color_by_h  = st.selectbox("Couleur", ["AQI (norme)", "Température"], key="color_hourly")

        # Hover horaire
        df_h["hover"] = df_h.apply(hover_hourly, axis=1)

        color_key_h = {"AQI (norme)": "aqi", "Température": "temperature"}[color_by_h]

        # Construction de la carte 
        fig_h = go.Figure()
        for _, row in df_h.iterrows():
            city_name = row["city"]
            if city_name not in CITIES_COORDS:
                continue
            mc = get_aqi_color(row["aqi"]) if color_by_h == "AQI (norme)" else get_temp_color(row["temperature"])
            fig_h.add_trace(go.Scattermapbox(
                lat=[CITIES_COORDS[city_name]["lat"]],
                lon=[CITIES_COORDS[city_name]["lon"]],
                mode="markers+text",
                marker=dict(size=25, color=mc, opacity=0.85),
                text=[city_name], textposition="top center",
                textfont=dict(size=11, color="#1a1a2e", family="Inter"),
                hovertext=row["hover"], hoverinfo="text",
                showlegend=False,
            ))

            fig_h.add_trace(go.Scattermapbox(
                lat=[CITIES_COORDS[city_name]["lat"]],
                lon=[CITIES_COORDS[city_name]["lon"]],
                mode="text",
                text=[f"{row['aqi']:.0f}" if pd.notna(row.get("aqi")) else ""],
                textfont=dict(size=10, color="white", family="Inter"),
                hoverinfo="skip", showlegend=False,
            ))

        fig_h.update_layout(
            mapbox=dict(style=map_style, center=dict(lat=46.6, lon=2.5), zoom=4.6),
            margin=dict(l=0, r=0, t=0, b=0), height=520,
        )
        with col_map_h:
            st.plotly_chart(fig_h, width='stretch')

        with col_ctrl_h:
            if color_by_h == "AQI (norme)":
                st.markdown("##### Échelle AQI")
                for rng, lbl, col in [
                    ("0–50",    "Bon",               "#00c853"),
                    ("51–100",  "Modéré",            "#f9a825"),
                    ("101–150", "Mauvais (sensibles)","#ef6c00"),
                    ("151–200", "Mauvais",            "#c62828"),
                    ("201-300", "Très mauvais",       "#6a1b9a"),
                    (">300",    "Dangereux",          "#6f0909"),
                    ("No data","","#BEBABA")
                ]:
                    st.markdown(
                        f'<div style="display:flex;align-items:center;gap:8px;margin:3px 0">'
                        f'<div style="width:16px;height:16px;border-radius:50%;background:{col}"></div>'
                        f'<span style="font-size:0.78rem"><b>{rng}</b> {lbl}</span></div>',
                        unsafe_allow_html=True
                    )
            else :   
                st.markdown("##### Échelle Témpérature")
                for rng, lbl, col in [
                    ("< -15",    "Froid Extrême", "#4A148C"),
                    ("-15–-5",  "Grand Froid", "#1565C0"),
                    ("-5-0", "Gel","#42A5F5"),
                    ("0-7", "Frais",   "#81C784"),
                    ("7-14", "Douceur", "#CDDC39"),
                    ("14-20", "Agréable", "#FFEB3B"),
                    ("20-25", "Chaud", "#FBC02D"),
                    ("25-30", "Très Chaud", "#FB8C00"),
                    ("30-35", "Forte Chaleur",  "#E53935"),
                    (">35",    "Chaleur Extrême", "#B71C1C"),
                    ("No data","","#BEBABA")
                ]:
                    st.markdown(
                        f'<div style="display:flex;align-items:center;gap:8px;margin:3px 0">'
                        f'<div style="width:16px;height:16px;border-radius:50%;background:{col}"></div>'
                        f'<span style="font-size:0.78rem"><b>{rng}</b> {lbl}</span></div>',
                        unsafe_allow_html=True
                    )    

        
        # ── Tableau horaire ────────────────────────────────────────────────
        st.markdown("---")
        st.markdown(f"### Tableau horaire — {heure_labels.get(selected_hour, selected_hour)}h")
        cols_show = ["city","aqi","alert_level","pm25","pm10","primary_pollutant",
                    "temperature","feels_like","humidity","pressure_hpa","wind_speed","uvi","weather"]
        cols_labels = ["Ville","AQI","Alerte pollution","PM2.5","PM10","Polluant majoritaire",
                    "Temp °C","Ressenti °C","Humidité %","Pression","Vent m/s","UVI","Météo"]
        df_h_show = df_h[[c for c in cols_show if c in df_h.columns]].copy()
        df_h_show.columns = cols_labels[:len(df_h_show.columns)]
        df_h_show["Alerte pollution"] = df_h_show["AQI"].apply(get_aqi_label) 
        df_h_show["Météo"] = df_h_show["Météo"].map(dicoEN_FR)
        # df_h_show["Alerte météo"] = df_h_show["Alerte météo"].map(dicoEN_FR)
        df_h_show["Alerte météo"] = df_h.apply(
            lambda row : weather_condition_details(
                feels_like = row["feels_like"],
                humidity= row["humidity"],
                wind_speed= row["wind_speed"],
                wind_gust= row["wind_gust_mps"],
                uvi = row["uvi"],
            )["details"], axis = 1)
        st.dataframe(df_h_show.set_index("Ville").sort_values("AQI", ascending=False), width='stretch')
    
        
        #  Graphique horaire
        # ajouter graph de l'évolution des variables sur la journée ==> données passée et forecast pour le jour J
        
        st.markdown("---")
        st.markdown("### Évolution horaire par ville (sélection dans le menu latéral)")
        # st.selectbox("Ville à afficher",sorted(CITIES_COORDS.keys()),key="selected_city_map")
        city_hour = st.session_state["selected_city"]
        df_plot = data_heure.loc[data_heure["city"] == city_hour].copy()
        
        # conseil
        actual_hour = df_plot["hour_formatted"].iloc[-1]
        st.markdown(f"##### Recommendations pour la dernière heure - {city_hour} ({actual_hour})")
        st.markdown(advisory_html(df_plot[df_plot["hour_formatted"]==actual_hour]['aqi'].mean()), unsafe_allow_html=True)
        st.markdown("---")
        
        # récupère les forecasts, convertit en date locale et renomme les col pour cohérence entre df
        meteo_forecast_list = df_plot['last_forecast_w'].iloc[0]
        air_forecast_list = df_plot['last_forecast_pol'].iloc[0]
        meteo_forecast = pd.json_normalize(meteo_forecast_list)
         
        air_forecast = pd.json_normalize(air_forecast_list)
        
        meteo_forecast.drop(["timestamp_utc","hour"],axis = 1,inplace = True)
        air_forecast.drop(["timestamp_utc","hour_utc"],axis = 1,inplace = True)
        
        meteo_forecast["city"] = city_hour
        air_forecast["city"] = city_hour
        
        meteo_forecast.rename(columns= {"date_paris" : "date",
                                        "hour_paris" : "hour",
                                        "temperature_celsius": "temperature",
                                        "feels_like_celsius" : "feels_like",
                                        "humidity_percent" : "humidity",
                                        "wind_speed_mps" : "wind_speed",
                                        "weather_main" : "weather"},inplace = True)
        air_forecast.rename(columns= {"hour_paris" : "hour"},inplace = True)
        
        air_forecast["datetime"] = pd.to_datetime(air_forecast["date"]) + pd.to_timedelta(air_forecast["hour"],unit = "h")
        meteo_forecast["datetime"] = pd.to_datetime(meteo_forecast["date"]) + pd.to_timedelta(meteo_forecast["hour"],unit = "h")
        df_plot["datetime"] = pd.to_datetime(df_plot["date"]) + pd.to_timedelta(df_plot["hour"],unit = "h")
        df_plot = df_plot.drop(["last_forecast_w","last_forecast_pol"],axis = 1)
        
        # fill avec fake data
        # df_plot = fill_df_jour(df=df_plot,datetime_col="datetime",start_date=None,end_date=None)
        
        df_plot = df_plot.sort_values("datetime")
        
        # df_plot.drop(["hour_utc","hour_utc_formatted"],axis = 1,inplace = True)
        # meteo_forecast.drop(["hour_utc","hour_utc_formatted"],axis = 1,inplace = True)
        
        
        last_obs_hr = df_plot["datetime"].iloc[-1]
        forecast_24 = df_plot["datetime"].iloc[-1]+ pd.Timedelta(hours=24)
        
        # merge forecast data then merge all datas
        colonnes_communes = list(set(meteo_forecast.columns) & set(air_forecast.columns))
        df_forecasts = pd.merge(meteo_forecast, air_forecast, on =colonnes_communes)

        df_full = pd.concat([df_plot, df_forecasts], axis = 0,ignore_index = True)
        # df_full = df_plot.drop_duplicates(subset = ["datetime"])
        
        idx_forecast_24 = df_full[df_full["datetime"] <= forecast_24].index[-1]
        idx_forecast_all = df_full["datetime"].index[-1]
        
        # for city in cities_in_plot:
        df_full = df_full.sort_values(by = ["datetime"])
        
        # slices du df 24-48
        slices = {
            "obs":    df_full[df_full["datetime"] <= last_obs_hr],
            "prev24": df_full.loc[:idx_forecast_24],
            "prev48": df_full.loc[:idx_forecast_all],
        }
        
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("Observé"):          st.session_state["horizon"] = "obs"
        with col2:
            if st.button("+ Prévisions 24h"): st.session_state["horizon"] = "prev24"
        with col3:
            if st.button("+ Prévisions 48h"): st.session_state["horizon"] = "prev48"
            
        if "horizon" not in st.session_state:
            st.session_state["horizon"] = "prev48"
            
        df_view = slices[st.session_state["horizon"]]    
        df_view["x_label"] = df_view["datetime"].dt.strftime("%d/%m %Hh")           
        x = df_view["x_label"]
        x_labels = x.tolist()
        tick_vals = x.tolist()
        tick_text = df_view["datetime"].dt.strftime("%Hh").tolist()
        
        # pour la faire la délimitation entre prévision et observés            
        first_forecast_x = meteo_forecast["datetime"].dt.strftime("%d/%m %Hh").iloc[0]
        forecast_idx = x_labels.index(first_forecast_x) if first_forecast_x in x_labels else None
        
        
        jours_fr = {
            "Monday": "Lun", "Tuesday": "Mar", "Wednesday": "Mer",
            "Thursday": "Jeu", "Friday": "Ven",
            "Saturday": "Sam", "Sunday": "Dim",
        }
    ########################################################
    #########PLOTS
    ########################################################
    
        aqi_hover = df_view.apply(
            lambda r: (
                f"<b>{(g(r, 'x_label', '?'))}</b><br>"
                f"─────────────────<br>"
                f" AQI : <b>{fmt(g(r,'aqi'), '.0f')}</b><br>"
                f"🟡 PM10 : <b>{fmt(g(r,'pm10'), '.1f')}</b><br>"
                f"🔵 PM2.5 : <b>{fmt(g(r,'pm25'), '.1f')}</b><br>"
                f"🔴 NO₂ : <b>{fmt(g(r,'no2'), '.1f')}</b><br>"
                f"🟢 O₃ : <b>{fmt(g(r,'o3'), '.1f')}</b>"
            ), axis=1
        ).tolist()

        temp_hover = df_view.apply(
            lambda r: (
                f"<b>{str.capitalize(g(r, 'city', '?'))}</b>  {g(r, 'date', '?')} — {g(r, 'hour_formatted', '?')}<br>"
                f"─────────────────<br>"
                f"🌡️ Temp : <b>{fmt(g(r,'temperature'), '.1f')}°C ( Ressentie : <b>{fmt(g(r,'feels_like'), '.1f')}°) </b><br>"
                f"💧 Humidité : <b>{fmt(g(r,'humidity'), '.0f')}%</b><br>"
                f"☀️ UVI : <b>{fmt(g(r,'uvi'), '.1f')} ({dicoEN_FR.get(g(r,'uvi_category','?'))})</b><br>"
                f"💨 Vent : <b>{fmt(g(r,'wind_speed'), '.1f')} m/s</b><br>"
                f"⏲ Pression : <b>{fmt(g(r,'pressure_hpa'), '.1f')}</b>"
            ), axis=1
        ).tolist()
        
        fig_weather = make_subplots(specs=[[{"secondary_y": True}]])
        fig_AQI = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Polluants
        fig_AQI.add_trace(go.Bar(
            x=x, y=df_view["aqi"].tolist(),
            name="AQI moyen",
            hovertext=aqi_hover, 
            hoverinfo="text",
            marker_color=df_view["aqi"].apply(get_aqi_color).tolist(),
        ),row=1, col=1,secondary_y=False)

        pol = {"pm25" : "PM25","pm10" : "PM10","no2" : "NO₂","o3":"O₃" }
        colors = ["#4895ef","orange","red","green"]
        for (col, label), color, idx in zip(pol.items(), colors, range(1, 5)):
            fig_AQI.add_trace(go.Scatter(
                x=x, y=df_view[col].tolist(),
                mode="lines+markers", 
                name=label,
                line=dict(width=2,color = color), marker=dict(size=7), hoverinfo="skip"
            ),row=1, col=1, secondary_y=True)
        
    
        # Température humidité
        fig_weather.add_trace(go.Bar(
            x=df_view["datetime"].dt.strftime("%d/%m %Hh"), 
            y=df_view["temperature"].tolist(),
            name="Temp °C",
            marker_color=df_view["feels_like"].apply(get_temp_color).tolist(), 
            hovertext=temp_hover, hoverinfo="text",
            # opacity=0.5,
        ), secondary_y=False)
        
        fig_weather.add_trace(go.Scatter(
            x=df_view["datetime"].dt.strftime("%d/%m %Hh"), 
            y=df_view["humidity"].tolist(),
            mode="lines+markers", 
            name="Humidité %",
            line=dict(width=2), marker=dict(size=7), hoverinfo="none"
        ), secondary_y=True)
        
        fig_weather.add_trace(go.Scatter(
                x=df_view["datetime"].dt.strftime("%d/%m %Hh"), 
                y=df_view["feels_like"].tolist(),
                name="Ressenti °C",
                mode="lines", hoverinfo='none',
                line=dict(color="gray", dash="dot", width=2),
            ), row=1, col=1, secondary_y=False)
        
        
        for fig in [fig_AQI,fig_weather] : 
            for i, day in enumerate(df_view["datetime"].dt.date.unique()):

                df_day = df_view[df_view["datetime"].dt.date == day]

                x0 = df_day["datetime"].dt.strftime("%d/%m %Hh").iloc[0]
                x1 = df_day["datetime"].dt.strftime("%d/%m %Hh").iloc[-1]
                day_name_en = pd.to_datetime(day).strftime("%A")
                day_name_fr = jours_fr[day_name_en]
                
                # compartiment/jour
                fig.add_vrect(
                    x0=x0,
                    x1=x1,
                    fillcolor="lightgrey" if i % 2 == 0 else "lightblue",
                    opacity=0.12,
                    layer="below",
                    line_width=0,
                    annotation_text=f"{day_name_fr} {pd.to_datetime(day).strftime('%d/%m')}",
                    annotation_position="top left"
                )
            # Ligne obs/prévisions
            if forecast_idx is not None:
                fig.add_shape(
                    type="line",
                    x0=forecast_idx - 0.5, x1=forecast_idx - 0.5,
                    y0=0, y1=1,
                    xref="x", yref="paper",
                    line=dict(dash="dash", color="black", width=2)
                )
                fig.add_annotation(
                    x=first_forecast_x,
                    y=1,
                    xref="x", yref="paper",
                    text="◀ Observé  |  Prévisions ▶",
                    showarrow=False,
                    yanchor="bottom",
                    font=dict(size=11, color="rgba(80,80,80,0.9)"),
                    borderwidth=1,
                    borderpad=1,
                )
            
            
            fig.add_trace(go.Scatter(
                x=[None], y=[None],          
                mode="lines",
                name="Observés | Prévisions",
                line=dict(dash="dash", color="black", width=2),
                legend="legend",            
            ))
        
        fig_weather.update_yaxes(title_text="°C", secondary_y=False, showgrid=False)
        fig_weather.update_yaxes(title_text="% Hum", secondary_y=True, showgrid=False)
        
        fig_AQI.update_yaxes(title_text="°AQI Moyen", secondary_y=False, showgrid=False)
        fig_AQI.update_yaxes(title_text="AQI Polluant", secondary_y=True, showgrid=False)  
        
        fig_weather.update_layout(
            xaxis_title="Heure locale",
            title="Température & Ressenti (°C)  —  Humidité (%)", height=380,
            hovermode="x",
            hoverdistance=100,
            barmode="group",
            xaxis = dict(
                tickmode = "array",
                tickvals = tick_vals[::3], 
                ticktext = tick_text[::3],
                showgrid = False
            ),
            margin=dict(l=10, r=10, t=40, b=20))
        
        fig_AQI.update_layout(
            title="AQI - Polluants", height=380,
            barmode="group",
            hovermode="x",
            hoverdistance=100,
            xaxis = dict(
                tickmode = "array",
                tickvals = tick_vals[::3], 
                ticktext = tick_text[::3],
                showgrid = False
            ),
            margin=dict(l=10, r=10, t=40, b=20))
        
        
        # grille vent,pression,uvi,precipitation
        n_rows_panel_W, n_cols_panel_W = 2, 2

        fig_panel_W = make_subplots(
            rows=n_rows_panel_W, cols=n_cols_panel_W,
            shared_xaxes=True,
            vertical_spacing=0.1,
            horizontal_spacing=0.1,
            specs=[
                [{}, {}],
                [{}, {}],
            ],
            subplot_titles=[
                "Vent moyen & Rafales (m/s)", "Pression (hPa)",
                "UVI", "Prob. Précipitations (%)",
            ],
            
        )

        # [3,1] Vent moyen + Rafales (barres groupées)
        fig_panel_W.add_trace(go.Scatter(
            x=x, y=df_view["wind_speed"],
            name="Vent moy (m/s)",
            marker_color="#457b9d",
            mode="lines+markers",
            hovertemplate="<b>%{x}</b><br>Vent : %{y:.1f} m/s<extra></extra>",
        ), row=1, col=1)
        fig_panel_W.add_trace(go.Scatter(
            x=x, y=df_view["wind_gust_mps"],
            name="Rafales (m/s)",
            mode="lines+markers",
            marker_color="rgba(230,57,70,0.55)",
            hovertemplate="<b>%{x}</b><br>Rafale : %{y:.1f} m/s<extra></extra>",
        ), row=1, col=1)

        # [3,2] Pression atmosphérique
        fig_panel_W.add_trace(go.Scatter(
            x=x, y=df_view["pressure_hpa"],
            name="Pression (hPa)",
            mode="lines+markers",
            line=dict(color="#6a4c93", width=2),
            marker=dict(size=4),
            hovertemplate="<b>%{x}</b><br>Pression : %{y:.0f} hPa<extra></extra>",
        ), row=1, col=2)

        # [4,1] UVI coloré par catégorie
        fig_panel_W.add_trace(go.Bar(
            x=x, y=df_view["uvi"],
            name="UVI",
            marker_color=df_view["uvi"].apply(uvi_color),
            hovertemplate="<b>%{x}</b><br>UVI : %{y:.1f}<extra></extra>",
        ), row=2, col=1)

        # [4,2] Probabilité précipitations + seuil d'alerte 80 %
        precip_col = "precipitation_probability_percent"
        if precip_col in df_view.columns:
            fig_panel_W.add_trace(go.Bar(
                x=x, y=df_view[precip_col],
                name="Précip. (%)",
                marker_color="lightblue",
                hovertemplate="<b>%{x}</b><br>Précip. : %{y:.0f}%<extra></extra>",
            ), row=2, col=2)
            fig_panel_W.add_hline(
                y=80, line_dash="dot",
                line_color="rgba(230,57,70,0.7)", line_width=1.5,
                row=2, col=2,
            )
        
        fig_panel_W.add_trace(go.Scatter(
                x=[None], y=[None],          
                mode="lines",
                name="Observés | Prévisions",
                line=dict(dash="dash", color="black", width=2),
                legend="legend",            
            ))

        # taille et légende
        fig_panel_W.update_layout(
            height=800,
            width = 1200,
            barmode="group",
            # margin=dict(l=10, r=10, t=60, b=10),
            legend=dict(orientation="v", y=1, x=1),
        )
        
        # annotationsn axes Y
        fig_panel_W.update_yaxes(title_text="m/s",    row=1, col=1, showgrid=False)
        fig_panel_W.update_yaxes(title_text="hPa",    row=1, col=2, showgrid=False, range=[780,1100])
        fig_panel_W.update_yaxes(title_text="UVI",    row=2, col=1, showgrid=False)
        fig_panel_W.update_yaxes(title_text="%",      row=2, col=2, showgrid=False, range=[0, 100])

        fig_panel_W.update_xaxes(
            tickmode="array", tickvals=tick_vals[::3], ticktext=tick_text[::3],
            showgrid=False, row=n_rows_panel_W, col=col,
        )
        
        # container jour
        for i, day in enumerate(df_view["datetime"].dt.date.unique()):
            df_day = df_view[df_view["datetime"].dt.date == day]
            x0_day = df_day["datetime"].dt.strftime("%d/%m %Hh").iloc[0]
            x1_day = df_day["datetime"].dt.strftime("%d/%m %Hh").iloc[-1]
            day_label = jours_fr[pd.to_datetime(day).strftime("%A")]
            for row in range(1, n_rows_panel_W + 1):
                for col in range(1, n_cols_panel_W + 1):
                    
                    fig_panel_W.add_vrect(
                        x0=x0_day, x1=x1_day,
                        fillcolor="lightgrey" if i % 2 == 0 else "lightblue",
                        opacity=0.10, layer="below", line_width=0,
                        # Annotation du jour uniquement sur la 1ère cellule
                        annotation_text=(
                            f"{day_label} {pd.to_datetime(day).strftime('%d/%m')}"
                            # if row == 1 and col == 1 else ""
                        ),
                        annotation_position="top left",
                        row=row, col=col,
                    )

        # Séparateur Observé / Prévisions
        if forecast_idx is not None:
            for row in range(1, n_rows_panel_W + 1):
                for col in range(1, n_cols_panel_W + 1):
                    fig_panel_W.add_shape(
                        type="line",
                        x0=forecast_idx - 0.5, x1=forecast_idx - 0.5,
                        y0=0, y1=1,
                        xref="x",yref="y domain",
                        line=dict(dash="dash", color="black", width=1.5),
                        row=row, 
                        col=col,
                    )
        
        
        # zoom sur toute les figures quand sélection
        st.plotly_chart(fig_weather, width="stretch")
        st.plotly_chart(fig_AQI, width="stretch")
        fig_panel_W.update_xaxes(matches = "x")
        st.plotly_chart(fig_panel_W, use_container_width=True)

        # # conseil
        # st.markdown("---")
        # st.markdown("#### Recommendations pour la dernière heure")
        # actual_hour = df_view["hour_formatted"].iloc[-1]
        # st.markdown(advisory_html(df_view[df_view["hour_formatted"]==actual_hour]['aqi'].mean()), unsafe_allow_html=True)
        # st.markdown("---")
        
    
        # Heatmap horaire multi-jours 
        # with st.expander("heatmap multi-jours", expanded=False):
        #     heatmap_vars = {
        #         "Température (°C)":          "temperature",
        #         "Humidité (%)":              "humidity",
        #         "UVI":                       "uvi",
        #         "Prob. Précipitations (%)":  "precipitation_probability_percent",
        #         "Vent moyen (m/s)":          "wind_speed",
        #         "Rafales (m/s)":             "wind_gust_mps",
        #         "Pression (hPa)":            "pressure_hpa",
        #         "AQI":                       "aqi",
        #         "PM2.5":            "pm25",
        #         "PM10":             "pm10",
        #         "NO₂":              "no2",
        #         "O₃":               "o3",
        #     }
        #     # Filtrer sur les colonnes réellement disponibles
        #     heatmap_vars = {k: v for k, v in heatmap_vars.items() if v in df_plot.columns}

        #     hm_col1, hm_col2 = st.columns([2, 1])
        #     with hm_col1:
        #         hm_var_label = st.selectbox("Variable", list(heatmap_vars.keys()), key="hm_var")
        #     with hm_col2:
                
        #         palette = st.selectbox(
        #             "Palette",
        #             ["RdYlGn","Blues"],
        #             key="hm_palette",
        #         )

        #     hm_col = heatmap_vars[hm_var_label]
        #     df_hm = df_plot[["datetime","hour", hm_col]].copy()
        #     df_hm["date"] = df_hm["datetime"].dt.strftime("%d/%m")

        #     df_pivot = df_hm.pivot_table(index="date", columns="hour", values=hm_col)
        #     # Conserver l'ordre chronologique des dates
        #     df_pivot = df_pivot.loc[df_hm["date"].unique()]
            
        #     fig_hm = go.Figure(go.Heatmap(
        #         z=df_pivot.values,
        #         x=[f"{h:02d}h" for h in df_pivot.columns],
        #         y=df_pivot.index.tolist(),
        #         colorscale=palette,
        #         colorbar=dict(title=hm_var_label, thickness=14),
        #         hoverongaps=False,
        #         hovertemplate="<b>%{y}  %{x}</b><br>" + hm_var_label + " : %{z:.1f}<extra></extra>",
        #     ))
        #     fig_hm.update_layout(
        #         title=f"Heatmap horaire — {hm_var_label} — {str.capitalize(city_hour)}",
        #         height=max(220, 80 * len(df_pivot)),   # hauteur adaptative selon nb de jours
        #         margin=dict(l=10, r=10, t=40, b=10),
        #         xaxis=dict(title="Heure locale", showgrid=False),
        #         yaxis=dict(title="Date", showgrid=False),
        #     )
        #     st.plotly_chart(fig_hm, use_container_width=True)
        

## call de la page        
# if __name__ == "__main__":

#     st.set_page_config(
#     page_title="Carte Interactive",
#     layout="wide"
# )
    #encache
    # # df_date = load_daily(date=today_date.isoformat())
    # #données agrégées
    # df_agg  = load_agg_data(date=sel_date.isoformat())
    
    # # charge df avec données horaire
    # data_heure = load_hourly_period(date = sel_date.isoformat(),city=None)