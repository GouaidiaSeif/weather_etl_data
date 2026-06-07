"""
Calculate US AQI from forecast data (μg/m³) with fallback to historical AQI if insufficient data.

Methods :
    - NO2, SO2 : direct calculation (1h average)
    - O3, CO   : 8h rolling average if ≥6 values available, else fallback on historical AQI
    - PM2.5    : 24h rolling average (simplified NowCast) if ≥3 values, else fallback
    - PM10     : same as PM2.5  
    - NH3, NO  : no EPA standard → ignored 
"""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from transformations.improved_air_quality_transformer import AirQualityTransformer

# ---------------------------------------------------------------------------
# Breakpoints EPA US (concentration_low, concentration_high, AQI_low, AQI_high)
# ---------------------------------------------------------------------------
 
BREAKPOINTS = {
    # PM2.5 (μg/m³) (24h)
    "pm2_5": [
        (0.0,   9.0,    0,  50),
        (9.1,  35.4,   51, 100),
        (35.5,  55.4,  101, 150),
        (55.5, 125.4,  151, 200),
        (125.5, 225.4, 201, 300),
        (225.5, 325.4, 301, 500),
    ],
    # PM10 (μg/m³) (24h)
    "pm10": [
        (0,    54,    0,  50),
        (55,   154,  51, 100),
        (155,  254, 101, 150),
        (255,  354, 151, 200),
        (355,  424, 201, 300),
        (425,  604, 301, 500),
    ],
    # NO2 (ppb) (1h)
    "no2": [
        (0,    53,    0,  50),
        (54,   100,  51, 100),
        (101,  360, 101, 150),
        (361,  649, 151, 200),
        (650,  1249, 201, 300),
        (1250, 2049, 301, 500),
    ],
    # SO2 (ppb) (1h)
    "so2": [
        (0,   35,    0,  50),
        (36,   75,  51, 100),
        (76,  185, 101, 150),
        (186, 304, 151, 200),
        (305, 604, 201, 300),
        (605, 1004, 301, 500),
    ],
    # O3 (ppb) (8h) — range 0-70 ppb only, above use 1h
    "o3_8h": [
        (0,    54,    0,  50),
        (55,   70,   51, 100),
        (71,   85,  101, 150),
        (86,  105,  151, 200),
        (106, 200,  201, 300),
    ],
    # CO (ppm) (8h)
    "co": [
        (0.0,  4.4,    0,  50),
        (4.5,  9.4,   51, 100),
        (9.5,  12.4, 101, 150),
        (12.5, 15.4, 151, 200),
        (15.5, 30.4, 201, 300),
        (30.5, 50.4, 301, 500),
    ],
}
# Conversion factors μg/m³ → EPA
CONVERSION = {
    "no2": 1 / 1.88,   # → ppb
    "so2": 1 / 2.62,   # → ppb
    "o3":  1 / 2.00,   # → ppb
    "co":  1 / 1145,   # → ppm
}

def _concentration_to_aqi(c: float, pollutant: str) -> int | None:
    """
    Convert a concentration (in EPA unit) to AQI.
    Returns None if out of breakpoints (data validation).
    """
    bps = BREAKPOINTS.get(pollutant)
    if bps is None:
        return None
    for c_low, c_high, i_low, i_high in bps:
        if c_low <= round(c, 1) <= c_high:
            aqi = (i_high - i_low) / (c_high - c_low) * (c - c_low) + i_low
            return round(aqi)
    return None

def _aqi_to_concentration(aqi: float, pollutant: str) -> float | None:
    """
    Convert an AQI value to a concentration (approximation, ±rounding error).
    """
    bps = BREAKPOINTS.get(pollutant)
    if bps is None:
        return None
    for c_low, c_high, i_low, i_high in bps:
        if i_low <= aqi <= i_high:
            c = (aqi - i_low) / (i_high - i_low) * (c_high - c_low) + c_low
            return round(c, 2)
    return None

def _sliding_mean(values: list[float], window: int, min_valid: int) -> float | None:
    """
    Calculate a sliding mean over the last `window` values.
    Returns None if not enough valid values (< min_valid).
    """
    recent = [v for v in values[-window:] if v is not None]
    if len(recent) < min_valid:
        return None
    return sum(recent) / len(recent)


def compute_forecast_aqi(
    forecast_components: dict,
    historical_hourly: list[dict],
) -> dict:
    """
    Calculate AQI for a given forecast hour.
 
    Args :
        forecast_components : field "components" of the forecast pollution data
        historical_hourly : gold pollution data, field "hourly_data"
 
    Returns : 
    Dict with AQI values for each pollutant, overall AQI and method used.
    """
 
    results = {}
    # methods = {}
    
    
     # --- Helpers pour récupérer l'historique d'un polluant ---
    def hist_values(field: str) -> list:
        return [h.get(field) for h in historical_hourly]
 
    def hist_aqi_values() -> list:
        return [h.get("aqi") for h in historical_hourly]
    
    
    # -------------------------------------------------------------------------
    # NO2 — moyenne 1h, calcul direct
    # -------------------------------------------------------------------------
    no2_ugm3 = forecast_components.get("no2")
    if no2_ugm3 is not None:
        no2_ppb = no2_ugm3 * CONVERSION["no2"]
        results["no2"] = _concentration_to_aqi(no2_ppb, "no2")
        # methods["no2"] = "direct_1h"
        
        
    # -------------------------------------------------------------------------
    # SO2 — moyenne 1h, calcul direct
    # -------------------------------------------------------------------------
    so2_ugm3 = forecast_components.get("so2")
    if so2_ugm3 is not None:
        so2_ppb = so2_ugm3 * CONVERSION["so2"]
        results["so2"] = _concentration_to_aqi(so2_ppb, "so2")
        # methods["so2"] = "direct_1h"
        
    # -------------------------------------------------------------------------
    # O3 — moyenne glissante 8h (≥6 valeurs) sinon fallback AQI historique
    # -------------------------------------------------------------------------
    o3_ugm3 = forecast_components.get("o3")
    if o3_ugm3 is not None:
        o3_ppb_forecast = o3_ugm3 * CONVERSION["o3"]
 
        # Historique : valeurs stockées sont des AQI → inverser en ppb
        hist_o3_aqi = hist_values("o3")
        hist_o3_ppb = [
            _aqi_to_concentration(v, "o3_8h") if v is not None else None
            for v in hist_o3_aqi
        ]
        # Ajoute la valeur prévue (en ppb) à la fin
        all_o3 = hist_o3_ppb + [o3_ppb_forecast]
        mean_o3 = _sliding_mean(all_o3, window=8, min_valid=6)
 
        if mean_o3 is not None:
            results["o3"] = _concentration_to_aqi(mean_o3, "o3_8h")
            # methods["o3"] = "sliding_8h"
        else:
            # Fallback : AQI historique moyen comme proxy
            hist_aqi = [v for v in hist_aqi_values() if v is not None]
            if hist_aqi:
                results["o3"] = round(sum(hist_aqi[-8:]) / len(hist_aqi[-8:]))
                # methods["o3"] = "fallback_hist_aqi"
            else:
                # Dernier recours : calcul direct 1h
                results["o3"] = _concentration_to_aqi(o3_ppb_forecast, "o3_8h")
                # methods["o3"] = "fallback_direct_1h"
      
                
    # -------------------------------------------------------------------------
    # CO — moyenne glissante 8h (≥6 valeurs) sinon fallback
    # -------------------------------------------------------------------------
    co_ugm3 = forecast_components.get("co")
    if co_ugm3 is not None:
        co_ppm_forecast = co_ugm3 * CONVERSION["co"]
 
        # Historique : valeurs stockées sont des AQI → inverser en ppm
        hist_co_aqi = hist_values("co")
        hist_co_ppm = [
            _aqi_to_concentration(v, "co") if v is not None else None
            for v in hist_co_aqi
        ]
        all_co = hist_co_ppm + [co_ppm_forecast]
        mean_co = _sliding_mean(all_co, window=8, min_valid=6)
 
        if mean_co is not None:
            results["co"] = _concentration_to_aqi(mean_co, "co")
            # methods["co"] = "sliding_8h"
        else:
            # Fallback direct 1h (CO varie peu, erreur limitée)
            results["co"] = _concentration_to_aqi(co_ppm_forecast, "co")
            # methods["co"] = "fallback_direct_1h"
            
            
    # -------------------------------------------------------------------------
    # PM2.5 — moyenne glissante 24h / NowCast simplifié (≥3 valeurs)
    # -------------------------------------------------------------------------
    pm25_ugm3 = forecast_components.get("pm2_5")
    if pm25_ugm3 is not None:
        # Historique : valeurs stockées sont des AQI → inverser en μg/m³
        hist_pm25_aqi = hist_values("pm25")
        hist_pm25_ugm3 = [
            _aqi_to_concentration(v, "pm2_5") if v is not None else None
            for v in hist_pm25_aqi
        ]
        all_pm25 = [v for v in hist_pm25_ugm3 if v is not None] + [pm25_ugm3]
        mean_pm25 = _sliding_mean(all_pm25, window=24, min_valid=3)
 
        if mean_pm25 is not None:
            results["pm25"] = _concentration_to_aqi(mean_pm25, "pm2_5")
            # methods["pm2_5"] = "sliding_24h"
        else:
            results["pm25"] = _concentration_to_aqi(pm25_ugm3, "pm2_5")
            # methods["pm2_5"] = "fallback_direct_1h"
            
    
    # -------------------------------------------------------------------------
    # PM10 — idem PM2.5
    # -------------------------------------------------------------------------
    pm10_ugm3 = forecast_components.get("pm10")
    if pm10_ugm3 is not None:
        # Historique : valeurs stockées sont des AQI → inverser en μg/m³
        hist_pm10_aqi = hist_values("pm10")
        hist_pm10_ugm3 = [
            _aqi_to_concentration(v, "pm10") if v is not None else None
            for v in hist_pm10_aqi
        ]
        all_pm10 = [v for v in hist_pm10_ugm3 if v is not None] + [pm10_ugm3]
        mean_pm10 = _sliding_mean(all_pm10, window=24, min_valid=3)
 
        if mean_pm10 is not None:
            results["pm10"] = _concentration_to_aqi(mean_pm10, "pm10")
            # methods["pm10"] = "sliding_24h"
        else:
            results["pm10"] = _concentration_to_aqi(pm10_ugm3, "pm10")
            # methods["pm10"] = "fallback_direct_1h"
            
            
            
    # -------------------------------------------------------------------------
    # AQI global = max des AQI polluants (convention EPA)
    # -------------------------------------------------------------------------
    valid_aqis = [v for v in results.values() if v is not None]
    global_aqi = max(valid_aqis) if valid_aqis else None
    primary = (
        max(results, key=lambda k: results[k] or 0)
        if valid_aqis else None
    )
 
    return {
        "aqi": global_aqi,
        "primary_pollutant": primary,
        "pm25" : results.get("pm25"),
        "pm10" : results.get("pm10"),
        "no2" : results.get("no2"),
        "so2" : results.get("so2"),
        "o3" : results.get("o3"),
        "co" : results.get("co")
        # "methods": methods,
    }
    

# ---------------------------------------------------------------------------
# Fonction d'intégration : enrichit le JSON du jour avec last_forecast
# ---------------------------------------------------------------------------
 
def add_forecast_to_daily_json(
    daily_json: dict,
    forecast_entries: dict,
) -> dict:
    """
    Enrichit le JSON quotidien avec un champ 'last_forecast' contenant
    les prévisions AQI calculées à partir de la dernière heure couverte.
 
    Parameters
    ----------
    daily_json : dict
        Le JSON du jour chargé depuis le fichier (ex: 2026-05-14.json).
 
    forecast_entries : dict
        Dictionnaire de prévisions horaires, chacune avec :
          - 'main'       : dict avec index AQI global (non utilisé ici) 
          - 'components' : dict des concentrations en μg/m³
          - 'dt'         : timestamp UNIX UTC
          - 'hour'       : heure locale (int)
 
    Returns
    -------
    dict : le JSON enrichi avec le champ 'last_forecast'.
    """
 
    historical_hourly = daily_json.get("hourly_data", [])
    hours_covered = daily_json.get("hours_covered", [])
    # last_hour_utc = max(hours_covered) if hours_covered else -1
    
    # Filtre : uniquement les prévisions après la dernière heure couverte
    future_entries = forecast_entries["list"][1:]
 
    last_forecast = []
    for entry in future_entries:
        components = entry.get("components", {})
        dt = entry.get("dt")
        timestamp_utc= (datetime.fromtimestamp(dt, tz=timezone.utc).isoformat()if dt else None)
        ts_local = datetime.fromtimestamp(int(dt), tz=timezone.utc).astimezone(ZoneInfo("Europe/Paris"))
        timestamp_local = ts_local.isoformat() if dt else None
        hour_utc = datetime.fromisoformat(timestamp_utc).hour if timestamp_utc else None
        hour_local = datetime.fromisoformat(timestamp_local).hour if timestamp_local else None
        aqi_result = compute_forecast_aqi(components, historical_hourly)
        date_paris = ts_local.strftime("%Y-%m-%d")
        
        # add alert level
        alert_level = AirQualityTransformer.calculate_alert_level(aqi_result["aqi"]) if aqi_result["aqi"] else None 
        health_risk = AirQualityTransformer.calculate_health_risk_score(aqi_result["aqi"],alert_level) if aqi_result["aqi"] else None
        
        last_forecast.append({
            # "dt": dt,
            "hour_utc": hour_utc,
            "hour" : hour_local,
            "hour_formatted": f"{hour_local:02d}:00" if hour_local is not None else None,
            "timestamp_utc": timestamp_utc,
            "timestamp_local": timestamp_local,
            "date" : date_paris,
            "aqi": aqi_result["aqi"],
            "primary_pollutant": aqi_result["primary_pollutant"],
            "pm25" : aqi_result["pm25"],
            "pm10" : aqi_result["pm10"],
            "no2" : aqi_result["no2"],
            "so2" : aqi_result["so2"],
            "o3" : aqi_result["o3"],
            "co" : aqi_result["co"],
            "health_risk" : health_risk,
            # "methods": aqi_result["methods"],
            # "components_ugm3": components,
        })
 
    enriched = dict(daily_json)
    enriched["last_forecast"] = last_forecast
    return enriched