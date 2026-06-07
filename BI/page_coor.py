import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json

import sys
from pathlib import Path
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from BI.data_loader import load_agg_data
from BI.helpers_BI import init_session_defaults, inject_css

init_session_defaults()
st.markdown(inject_css(), unsafe_allow_html=True)


st.set_page_config(
        page_title="Corrélations",
        layout="wide",
    )

st.title('Analyse des Corrélations')
st.subheader('Identification des variables fortement corrélées avec la qualité de l\'air')

# toute les données depuis 04-1
df_main  = load_agg_data(date_start="2026-04-01",date_end="2026-07-30")
df_main["w_precipitation_detected"] = df_main["w_precipitation_detected"].replace({True : 1,
                                                                            False : 0})

NUM_COLS  = ["pol_avg_aqi","pol_aqi_volatility","pol_avg_pm25","pol_avg_pm10","pol_avg_no2","pol_avg_o3","pol_avg_co","pol_avg_so2","pol_avg_health_risk_score"
                ,"w_avg_temperature","w_avg_feels_like","w_avg_humidity","w_avg_pressure","w_avg_wind_speed","w_avg_uvi","w_precipitation_detected"]
NICE_LABS = ["AQI","Volatilité AQI","PM2.5","PM10","NO₂","O₃","CO","SO₂","Risque santé",
                "Température","Température_ressentie","Humidité","Pression","Vent","UV","Précipitation"]

lab_map   = dict(zip(NUM_COLS, NICE_LABS))

# filter_city_corr = st.multiselect("Filtrer par ville (laisser vide = toutes)", sorted(df_main["city"].unique()))
# df_c = df_main[df_main["city"].isin(filter_city_corr)] if filter_city_corr else df_main
df_c = df_main
df_corr_data = df_c[NUM_COLS].dropna()

# @st.cache_data(ttl=300)
def compute_correlation_matrix(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """
    Compute correlation matrix
    """
    return df[cols].dropna().corr()

corr = compute_correlation_matrix(df_c, NUM_COLS)
col_heat, col_rank = st.columns([3, 2])

# with col_heat:
st.markdown("####  Matrice de Corrélation")
ztext = [[f"{v:.2f}" for v in row] for row in corr.values]
fig_heat = go.Figure(go.Heatmap(
    z=corr.values, x=NICE_LABS, y=NICE_LABS,
    colorscale="RdBu_r", zmid=0,
    text=ztext, texttemplate="%{text}", textfont={"size": 10},
    showscale=True, zmin=-1, zmax=1,
))
fig_heat.update_layout(height=600, margin=dict(l=10,r=10,t=10,b=10))
st.plotly_chart(fig_heat, width='stretch')

# with col_rank:
st.markdown("####  Corrélations avec l'AQI")
aqi_corr = corr["pol_avg_aqi"].drop("pol_avg_aqi").sort_values(key=abs, ascending=False)
for var, val in aqi_corr.items():
    bar_col = "#ef233c" if val > 0 else "#3a86ff"
    bar_w   = abs(val) * 100
    strength = "Forte" if abs(val) > 0.65 else "Modérée" if abs(val) > 0.35 else "Faible"
    icon     = "🔺" if val > 0 else "🔻"
    st.markdown(f"""
    <div style="background:#f8f9fa;border-radius:8px;padding:8px 12px;margin:5px 0;border-left:4px solid {bar_col}">
        <b>{lab_map[var]}</b>
        <span style="float:right;font-size:0.8rem;color:{bar_col}">{icon} {val:.3f} — {strength}</span>
        <div class="corr-bar" style="background:{bar_col};width:{bar_w}%;opacity:0.5"></div>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")
st.markdown("####  Nuage de Points Bi-Varié")
sc1, sc2, sc3 = st.columns([1,1,1])
with sc1: x_var = st.selectbox("Variable X", NUM_COLS, index=5, format_func=lambda x: lab_map[x])
with sc2: y_var = st.selectbox("Variable Y", NUM_COLS, index=0, format_func=lambda x: lab_map[x])
with sc3: color_var = st.selectbox("Couleur", ["city"] + NUM_COLS, format_func=lambda x: x if x == "city" else lab_map[x])

fig_sc = px.scatter(
    df_c, x=x_var, y=y_var,
    color=color_var,
    trendline="ols",
    labels={x_var: lab_map[x_var], y_var: lab_map[y_var]},
    hover_data=["city","date"],
    color_continuous_scale="Viridis" if color_var != "city" else None,
)
r_val = corr.loc[x_var, y_var] if x_var in corr.index and y_var in corr.columns else 0
fig_sc.update_layout(height=380, title=f"r = {r_val:.3f}  |  {lab_map[x_var]} vs {lab_map[y_var]}", margin=dict(l=20,r=20,t=50,b=20))
st.plotly_chart(fig_sc, width='stretch')


