import streamlit as st
import pandas as pd
import numpy as np

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

METEO_HOURLY_COLS = {
    # colonne             (moyenne_annuelle, amplitude_saisonnière, bruit_std)
    "temperature":        (14.0,  10.0, 1.5),
    "feels_like":         (11.0,  10.0, 2.0),
    "humidity":           (70.0,  15.0, 5.0),
    "pressure_hpa":       (1013.0, 5.0, 1.0),
    "wind_speed":         (4.0,    2.0, 1.0),
    "wind_gust_mps":      (7.0,    3.0, 1.5),
    "uvi":                (2.5,    2.5, 0.3),   # recalculé selon heure du jour
    "precipitation_probability_percent": (40.0, 20.0, 10.0),
    "aqi":                (35.0,  15.0, 5.0),
    "pm25":               (8.0,    4.0, 1.5),
    "pm10":               (12.0,   5.0, 2.0),
    "no2":                (15.0,   8.0, 3.0),
    "o3":                 (40.0,  20.0, 5.0),
}

METEO_DAILY_COLS = {
    # colonne             (moyenne_annuelle, amplitude_saisonnière, bruit_std)
    "w_avg_temperature":    (14.0, 10.0, 1.0),
    "w_max_temperature":    (20.0, 10.0, 1.5),
    "w_min_temperature":    (8.0,  10.0, 1.5),
    "w_avg_humidity":       (70.0, 15.0, 4.0),
    "w_avg_pressure":       (1013.0, 5.0, 1.0),
    "w_avg_wind_speed":     (4.0,   2.0, 0.8),
    "w_max_wind_gust":      (9.0,   3.0, 1.5),
    "w_max_uvi":            (4.0,   3.5, 0.5),
    "pol_avg_aqi":            (50.0, 15.0, 4.0),
    "pol_min_aqi":            (20.0, 15.0, 4.0),
    "pol_max_aqi":            (100.0, 15.0, 4.0),
    "pol_avg_pm25":           (8.0,   4.0, 1.0),
    "pol_avg_no2":           (12.0,  5.0, 1.5),
    "pol_avg_o3":           (12.0,  5.0, 1.5),
}

CITIES = ["toulouse", "paris", "bordeaux","marseille","lyon"
          ,"nice","nantes","strasbourg","lille","montpellier"]


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _seasonal_value(dt: pd.Timestamp, mean: float, amplitude: float, noise_std: float) -> float:
    """Valeur simulée avec cycle saisonnier sinusoïdal + bruit gaussien."""
    day_of_year = dt.day_of_year
    seasonal = amplitude * np.sin(2 * np.pi * (day_of_year - 80) / 365)
    noise = np.random.normal(0, noise_std)
    return float(np.clip(mean + seasonal + noise, 0, None))


def _uvi_from_hour(dt: pd.Timestamp, base_uvi: float) -> float:
    """UVI nul la nuit, pic à midi, modulé par saison."""
    h = dt.hour
    if h < 6 or h > 20:
        return 0.0
    sun_curve = np.sin(np.pi * (h - 6) / 14)   # 0→1→0 entre 6h et 20h
    return float(np.clip(base_uvi * sun_curve, 0, 11))


def _wind_direction(rng: np.random.Generator) -> str:
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW", "WNW", "WSW", "NNE", "SSW"]
    return rng.choice(directions)


def _weather_condition(precip_prob: float, uvi: float) -> str:
    if precip_prob > 70:
        return "rain"
    if precip_prob > 40:
        return "clouds"
    if uvi > 4:
        return "clear"
    return "clouds"


def _uvi_category(uvi: float) -> str:
    if uvi < 3:   return "low"
    if uvi < 6:   return "moderate"
    if uvi < 8:   return "high"
    if uvi < 11:  return "very_high"
    return "extreme"


# ══════════════════════════════════════════════════════════════════════════════
# 1. COMPLÉTION + GÉNÉRATION — df_jour (horaire)
# ══════════════════════════════════════════════════════════════════════════════

def fill_df_jour(
    df: pd.DataFrame,
    datetime_col: str = "datetime",
    city_col: str = "city",
    start_date: str | None = None,
    end_date: str | None = None,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Comble les heures manquantes et étend df_jour sur la plage [start_date, end_date].

    - Les heures existantes sont conservées telles quelles.
    - Les heures manquantes sont générées avec un cycle saisonnier réaliste.
    - Si start_date/end_date ne sont pas fournis, utilise le min/max du df existant.

    Paramètres
    ----------
    df           : DataFrame horaire existant (peut être vide)
    datetime_col : nom de la colonne datetime
    city_col     : nom de la colonne ville (si présente)
    start_date   : "YYYY-MM-DD" — début de la plage cible
    end_date     : "YYYY-MM-DD" — fin de la plage cible
    seed         : graine aléatoire pour reproductibilité
    """
    rng = np.random.default_rng(seed)
    df = df.copy()
    df[datetime_col] = pd.to_datetime(df[datetime_col])

    # Plage cible
    start = pd.to_datetime(start_date) if start_date else df[datetime_col].min().floor("D")
    end   = pd.to_datetime(end_date)   if end_date   else df[datetime_col].max().ceil("D")
    full_range = pd.date_range(start, end, freq="h")

    cities = df[city_col].unique().tolist() if city_col in df.columns else ["unknown"]
    frames = []

    for city in cities:
        df_city = df[df[city_col] == city].copy() if city_col in df.columns else df.copy()
        existing_dts = set(df_city[datetime_col])
        missing_dts  = [dt for dt in full_range if dt not in existing_dts]

        if not missing_dts:
            frames.append(df_city)
            continue

        # Générer les lignes manquantes
        rows = []
        for dt in missing_dts:
            row = {datetime_col: dt}
            if city_col in df.columns:
                row[city_col] = city

            base_uvi = _seasonal_value(dt, *METEO_HOURLY_COLS["uvi"])
            precip   = _seasonal_value(dt, *METEO_HOURLY_COLS["precipitation_probability_percent"])
            precip   = float(np.clip(precip, 0, 100))
            uvi      = _uvi_from_hour(dt, base_uvi)

            for col, params in METEO_HOURLY_COLS.items():
                if col == "uvi":
                    row["uvi"] = round(uvi, 2)
                elif col == "precipitation_probability_percent":
                    row[col] = round(precip, 1)
                else:
                    row[col] = round(_seasonal_value(dt, *params), 2)

            row["uvi_category"]   = _uvi_category(uvi)
            row["wind_direction_cardinal"] = _wind_direction(rng)
            row["weather"]        = _weather_condition(precip, uvi)
            row["weather_severity"] = "normal" if precip < 70 else "moderate"
            

            rows.append(row)

        df_fake = pd.DataFrame(rows)
        df_city = pd.concat([df_city, df_fake], ignore_index=True)
        df_city["hour"] = df_city[datetime_col].dt.hour
        df_city["hour_formatted"] = df_city["hour"].apply(lambda h: f"{h:02d}:00")
        df_city["date"] = df_city[datetime_col].dt.date
        frames.append(df_city)

    result = pd.concat(frames, ignore_index=True)
    result = result.sort_values(datetime_col).reset_index(drop=True)
    
    return result


# ══════════════════════════════════════════════════════════════════════════════
# 2. COMPLÉTION + GÉNÉRATION — df_agg (journalier)
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_data()
def fill_df_agg(
    df: pd.DataFrame,
    date_col: str = "date",
    city_col: str = "city",
    start_date: str | None = None,
    end_date: str | None = None,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Comble les jours manquants et étend df_agg sur la plage [start_date, end_date].

    - Les jours existants sont conservés tels quels.
    - Les jours manquants sont générés avec un cycle saisonnier réaliste.

    Paramètres
    ----------
    df         : DataFrame journalier existant (peut être vide)
    date_col   : nom de la colonne date
    city_col   : nom de la colonne ville (si présente)
    start_date : "YYYY-MM-DD" — début de la plage cible
    end_date   : "YYYY-MM-DD" — fin de la plage cible
    seed       : graine aléatoire pour reproductibilité
    """
    rng = np.random.default_rng(seed)
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])

    start = pd.to_datetime(start_date) if start_date else df[date_col].min()
    end   = pd.to_datetime(end_date)   if end_date   else df[date_col].max()
    full_range = pd.date_range(start, end, freq="D")

    cities = df[city_col].unique().tolist() if city_col in df.columns else ["unknown"]
    frames = []

    for city in cities:
        df_city = df[df[city_col] == city].copy() if city_col in df.columns else df.copy()
        existing_dates = set(df_city[date_col])
        missing_dates  = [d for d in full_range if d not in existing_dates]

        if not missing_dates:
            frames.append(df_city)
            continue

        rows = []
        for dt in missing_dates:
            row = {date_col: dt}
            if city_col in df.columns:
                row[city_col] = city

            for col, params in METEO_DAILY_COLS.items():
                row[col] = round(_seasonal_value(dt, *params), 2)

            # Cohérence min/max température
            avg = row.get("w_avg_temperature", 14.0)
            row["w_max_temperature"] = round(avg + abs(rng.normal(4, 1.5)), 2)
            row["w_min_temperature"] = round(avg - abs(rng.normal(4, 1.5)), 2)

            row["w_dominant_weather"] = _weather_condition(
                rng.uniform(0, 100),
                row.get("w_max_uvi", 3.0)
            )
            rows.append(row)

        df_fake = pd.DataFrame(rows)
        df_city = pd.concat([df_city, df_fake], ignore_index=True)
        frames.append(df_city)

    result = pd.concat(frames, ignore_index=True)
    result = result.sort_values(date_col).reset_index(drop=True)
    return result


# # ══════════════════════════════════════════════════════════════════════════════
# # USAGE
# # ══════════════════════════════════════════════════════════════════════════════
# if __name__ == "__main__":
#     # ── Créer un df_jour partiel pour tester ──────────────────────────────────
#     df_jour_partiel = pd.DataFrame({
#         "datetime": pd.date_range("2026-03-01", periods=48, freq="h"),
#         "city":     "toulouse",
#         "temperature": np.random.normal(12, 3, 48),
#     })

#     df_jour_complet = fill_df_jour(
#         df_jour_partiel,
#         start_date="2025-01-01",
#         end_date="2025-12-31",
#     )
#     print(f"df_jour : {len(df_jour_complet)} lignes | "
#           f"{df_jour_complet['datetime'].min()} → {df_jour_complet['datetime'].max()}")
#     print(df_jour_complet.head())
    
    
#     # ── Créer un df_agg partiel pour tester ───────────────────────────────────
#     df_agg_partiel = pd.DataFrame({
#         "date": pd.date_range("2026-06-01", periods=10, freq="D"),
#         "city": "toulouse",
#         "avg_temperature": np.random.normal(22, 3, 10),
#     })

#     df_agg_complet = fill_df_agg(
#         df_agg_partiel,
#         start_date="2025-01-01",
#         end_date="2025-12-31",
#     )
#     print(f"df_agg  : {len(df_agg_complet)} lignes | "
#           f"{df_agg_complet['date'].min().date()} → {df_agg_complet['date'].max().date()}")