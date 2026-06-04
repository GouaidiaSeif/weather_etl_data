"""
demo_data.py — Données de démonstration pour render_alerts_page()

Usage dans page_alertes.py :
    from demo_data import load_demo_data

    df = load_demo_data()
    weather_cdt = df.apply(
        lambda row: weather_condition_details(
            feels_like=row["feels_like_celsius"],
            humidity=row["humidity_percent"],
            wind_speed=row["wind_speed_mps"],
            wind_gust=row["wind_gust_mps"],
            uvi=row["uvi"],
        ), axis=1
    )
    df = pd.concat([df, pd.DataFrame(weather_cdt.tolist())], axis=1)
    render_alerts_page(df)

Scénario narratif (48h) :
    H00–H05   Nuit calme — baseline confortable
    H06       Chute de pression –3.5 hPa/3h     → delta Pression    moderate
    H08       Chute de pression –5.0 hPa/3h     → delta Pression    severe
    H09       Baisse température –5.2°C/1h       → delta Température severe
    H10       Rebond température +3.3°C/1h       → delta Température moderate
    H12       "violent thunderstorm"             → keyword thunderstorm + violent
    H13       "heavy rain"                       → keyword heavy
    H14       Rafales +5.5 m/s/1h               → delta Rafales 1h  moderate
    H15       Rafales +8.5 m/s/1h               → delta Rafales 1h  severe
    H17       Rafales +8.0 m/s/3h               → delta Rafales 3h  moderate
    H20       Vent     +4.5 m/s/3h              → delta Vent    3h  moderate
    H22       Chaleur + humidité + vent          → level             severe
    H24       Conditions extrêmes                → level             extreme
    H26       "orage électrique"                 → keyword orage
    H28       "violent storm"                    → keyword violent
    H29–H35   Accalmie progressive               → level             moderate
    H36–H47   Retour au calme                    → level             comfortable
"""

from datetime import datetime, timedelta
import pandas as pd


def load_demo_data() -> pd.DataFrame:
    """
    Retourne un DataFrame de 48 lignes prêt à être passé à render_alerts_page()
    après enrichissement par weather_condition_details.

    Colonnes produites : identiques au DataFrame issu de load_hourly_collection()
    après le rename hour_paris → hour (tel que dans le __main__ de page_alertes.py).
    """
    BASE_DATE = "2026-06-04"
    BASE_TS   = datetime(2026, 6, 4, 0, 0, 0)

    def _row(h: int, **kw) -> dict:
        """Ligne de base confortable, surchargeable via **kw."""
        r = {
            "timestamp_utc":                    (BASE_TS + timedelta(hours=h - 2)).isoformat() + "+00:00",
            "timestamp_paris":                  (BASE_TS + timedelta(hours=h)).isoformat() + "+02:00",
            "date_paris":                        BASE_DATE,
            "hour":                              h,
            "hour_formatted":                    f"{h:02d}:00",
            "city":                              "demo_city",
            "temperature_celsius":               16.0,
            "feels_like_celsius":                16.0,
            "dew_point_celsius":                 10.0,
            "humidity_percent":                  60,
            "pressure_hpa":                      1013.0,
            "wind_speed_mps":                    3.0,
            "wind_gust_mps":                     5.0,
            "wind_direction_deg":                200,
            "wind_direction_cardinal":           "SSW",
            "cloud_coverage_percent":            30,
            "visibility_m":                      10000,
            "weather_main":                      "clouds",
            "weather_description":               "few clouds",
            "weather_icon":                      "02d",
            "weather_id":                        801,
            "precipitation_probability_percent": 10,
            "uvi":                               2.0,
            "uvi_category":                      "low",
            "heat_index_celsius":                None,
            "weather_severity":                  "normal",
            "heat_index_warning":                "safe",
        }
        r.update(kw)
        return r

    rows = []

    # ── H00–H05 : nuit calme ──────────────────────────────────────────────────
    for h in range(6):
        rows.append(_row(h, pressure_hpa=1013.0, wind_speed_mps=3.0, wind_gust_mps=5.0))

    # ── H06 : delta Pression moderate (–3.5 hPa/3h vs H03) ───────────────────
    rows.append(_row(6, pressure_hpa=1009.5, wind_speed_mps=4.0, wind_gust_mps=6.0))

    # ── H07 : stable ─────────────────────────────────────────────────────────
    rows.append(_row(7, pressure_hpa=1009.5, wind_speed_mps=4.0, wind_gust_mps=6.0))

    # ── H08 : delta Pression severe (–5.0 hPa/3h vs H05) ────────────────────
    rows.append(_row(8,
        pressure_hpa=1008.0, wind_speed_mps=5.0, wind_gust_mps=7.0,
        temperature_celsius=18.5, feels_like_celsius=18.0,
    ))

    # ── H09 : delta Température severe (–5.2°C/1h vs H08) ───────────────────
    rows.append(_row(9,
        pressure_hpa=1007.0, wind_speed_mps=6.0, wind_gust_mps=9.0,
        temperature_celsius=13.3, feels_like_celsius=11.5,
    ))

    # ── H10 : delta Température moderate (+3.3°C/1h vs H09) ─────────────────
    rows.append(_row(10,
        pressure_hpa=1006.5, wind_speed_mps=6.0, wind_gust_mps=9.0,
        temperature_celsius=16.6, feels_like_celsius=15.0,
    ))

    # ── H11 : stable, ciel s'épaissit ────────────────────────────────────────
    rows.append(_row(11,
        pressure_hpa=1006.0, wind_speed_mps=6.5, wind_gust_mps=9.5,
        temperature_celsius=16.0, feels_like_celsius=14.8,
        weather_description="overcast clouds",
        cloud_coverage_percent=90, humidity_percent=72,
    ))

    # ── H12 : keyword "thunderstorm" + "violent" ─────────────────────────────
    rows.append(_row(12,
        pressure_hpa=1005.5, wind_speed_mps=9.0, wind_gust_mps=13.0,
        temperature_celsius=15.0, feels_like_celsius=13.0,
        weather_main="thunderstorm", weather_description="violent thunderstorm",
        weather_icon="11d", precipitation_probability_percent=95,
        humidity_percent=85, cloud_coverage_percent=100,
    ))

    # ── H13 : keyword "heavy" ────────────────────────────────────────────────
    rows.append(_row(13,
        pressure_hpa=1005.0, wind_speed_mps=9.0, wind_gust_mps=13.5,
        temperature_celsius=14.5, feels_like_celsius=12.8,
        weather_main="rain", weather_description="heavy rain",
        weather_icon="10d", precipitation_probability_percent=100,
        humidity_percent=90, cloud_coverage_percent=100,
    ))

    # ── H14 : delta Rafales 1h moderate (+5.5 m/s vs H13 : 13.5→19.0) ───────
    rows.append(_row(14,
        pressure_hpa=1004.5, wind_speed_mps=11.0, wind_gust_mps=19.0,
        temperature_celsius=14.0, feels_like_celsius=12.0,
        weather_main="rain", weather_description="heavy rain",
        precipitation_probability_percent=100, humidity_percent=90,
    ))

    # ── H15 : delta Rafales 1h severe (+8.5 m/s vs H14 : 19.0→27.5) ─────────
    rows.append(_row(15,
        pressure_hpa=1004.0, wind_speed_mps=14.0, wind_gust_mps=27.5,
        temperature_celsius=13.5, feels_like_celsius=10.5,
        weather_main="rain", weather_description="heavy rain",
        precipitation_probability_percent=100, humidity_percent=88,
    ))

    # ── H16 : stable post-pic, référence pour delta 3h rafales ───────────────
    rows.append(_row(16,
        pressure_hpa=1004.0, wind_speed_mps=14.0, wind_gust_mps=14.0,
        temperature_celsius=13.5, feels_like_celsius=11.0,
        humidity_percent=85, cloud_coverage_percent=95,
    ))

    # ── H17 : delta Rafales 3h moderate (+8.0 m/s vs H14 : 19.0→27.0) ───────
    rows.append(_row(17,
        pressure_hpa=1004.5, wind_speed_mps=15.0, wind_gust_mps=27.0,
        temperature_celsius=14.0, feels_like_celsius=11.5,
        humidity_percent=83, cloud_coverage_percent=90,
    ))

    # ── H18–H19 : vent fort, référence pour delta Vent 3h ────────────────────
    for h, ws, wg in [(18, 12.0, 20.0), (19, 10.0, 15.0)]:
        rows.append(_row(h,
            pressure_hpa=1005.0, wind_speed_mps=ws, wind_gust_mps=wg,
            temperature_celsius=14.5, feels_like_celsius=12.5, humidity_percent=80,
        ))

    # ── H20 : delta Vent 3h moderate (+4.5 m/s vs H17 : 15.0→19.5) ──────────
    rows.append(_row(20,
        pressure_hpa=1005.5, wind_speed_mps=19.5, wind_gust_mps=22.0,
        temperature_celsius=15.0, feels_like_celsius=13.0, humidity_percent=78,
    ))

    # ── H21 : stable, montée en température ──────────────────────────────────
    rows.append(_row(21,
        pressure_hpa=1006.0, wind_speed_mps=17.0, wind_gust_mps=20.0,
        temperature_celsius=16.0, feels_like_celsius=14.0, humidity_percent=75,
    ))

    # ── H22 : level severe
    #    feels_like=32 → +2 | humidity=87 → +2 | gust=25 → +2  ⇒ score 6 ──────
    rows.append(_row(22,
        pressure_hpa=1006.5, wind_speed_mps=18.0, wind_gust_mps=25.0,
        temperature_celsius=33.0, feels_like_celsius=32.0,
        humidity_percent=87, uvi=4.0,
        weather_description="overcast clouds", cloud_coverage_percent=100,
    ))

    # ── H23 : montée vers extreme ────────────────────────────────────────────
    rows.append(_row(23,
        pressure_hpa=1007.0, wind_speed_mps=20.0, wind_gust_mps=28.0,
        temperature_celsius=40.0, feels_like_celsius=40.0,
        humidity_percent=87, uvi=5.0,
    ))

    # ── H24 : level extreme
    #    feels_like=46 → +4 | humidity=87 → +2 | gust=31 → +3  ⇒ score 9 ──────
    rows.append(_row(24,
        pressure_hpa=1007.0, wind_speed_mps=25.0, wind_gust_mps=31.0,
        temperature_celsius=47.0, feels_like_celsius=46.0,
        humidity_percent=87, uvi=5.0,
        weather_description="overcast clouds", cloud_coverage_percent=100,
    ))

    # ── H25 : encore extreme ─────────────────────────────────────────────────
    rows.append(_row(25,
        pressure_hpa=1007.5, wind_speed_mps=23.0, wind_gust_mps=29.0,
        temperature_celsius=45.0, feels_like_celsius=44.0,
        humidity_percent=86, uvi=4.0,
    ))

    # ── H26 : keyword "orage" ────────────────────────────────────────────────
    rows.append(_row(26,
        pressure_hpa=1008.0, wind_speed_mps=15.0, wind_gust_mps=22.0,
        temperature_celsius=28.0, feels_like_celsius=30.0,
        weather_main="thunderstorm", weather_description="orage électrique",
        weather_icon="11d", precipitation_probability_percent=90,
        humidity_percent=85, cloud_coverage_percent=100,
    ))

    # ── H27 : stable post-orage ───────────────────────────────────────────────
    rows.append(_row(27,
        pressure_hpa=1008.5, wind_speed_mps=12.0, wind_gust_mps=17.0,
        temperature_celsius=24.0, feels_like_celsius=25.0,
        humidity_percent=80, cloud_coverage_percent=85,
    ))

    # ── H28 : keyword "violent" ──────────────────────────────────────────────
    rows.append(_row(28,
        pressure_hpa=1009.0, wind_speed_mps=13.0, wind_gust_mps=18.0,
        temperature_celsius=22.0, feels_like_celsius=22.0,
        weather_main="rain", weather_description="violent storm",
        precipitation_probability_percent=75,
        humidity_percent=78, cloud_coverage_percent=90,
    ))

    # ── H29–H35 : accalmie progressive → level moderate ──────────────────────
    for h, ws, wg, temp, hum in [
        (29, 20.0, 22.0, 20.0, 75),
        (30, 18.0, 20.0, 18.0, 72),
        (31, 16.0, 18.0, 16.5, 70),
        (32, 14.0, 16.0, 15.5, 68),
        (33, 12.0, 14.0, 15.0, 65),
        (34,  9.0, 11.0, 14.5, 63),
        (35,  7.0,  9.0, 14.0, 62),
    ]:
        rows.append(_row(h,
            pressure_hpa=round(1009.0 + (h - 29) * 0.3, 1),
            wind_speed_mps=ws, wind_gust_mps=wg,
            temperature_celsius=temp, feels_like_celsius=round(temp - 1.5, 1),
            humidity_percent=hum,
        ))

    # ── H36–H47 : retour au calme → level comfortable ────────────────────────
    for h in range(36, 48):
        rows.append(_row(h,
            pressure_hpa=1011.0, wind_speed_mps=3.0, wind_gust_mps=5.5,
            temperature_celsius=15.0, feels_like_celsius=15.0,
            humidity_percent=58, uvi=1.5,
            weather_description="scattered clouds",
        ))

    return pd.DataFrame(rows)
