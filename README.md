# Pipeline ETL météo & qualité de l'air

Pipeline ETL **horaire** pour la France : collecte **OpenWeatherMap** (météo) et **AQICN** (qualité de l'air) sur **10 grandes villes**, stockage **medallion** (Bronze / Silver / Gold), **nettoyage et validation** des données, **agrégats journaliers**, **alertes Discord**, et module **Machine Learning** (prédiction / anomalies AQI).

Toutes les heures métier sont alignées sur **`Europe/Paris`**.

---

## Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Démarrage rapide](#démarrage-rapide)
3. [Architecture du pipeline](#architecture-du-pipeline)
4. [Couche Bronze (raw)](#couche-bronze-raw)
5. [Couche Silver (nettoyage)](#couche-silver-nettoyage)
6. [Couche Gold (agrégation)](#couche-gold-agrégation)
7. [Système d'alertes Discord](#système-dalertes-discord)
8. [Configuration (.env)](#configuration-env)
9. [Guide d'utilisation](#guide-dutilisation)
10. [Stockage MongoDB & MinIO](#stockage-mongodb--minio)
11. [Machine Learning](#machine-learning)
12. [Villes couvertes](#villes-couvertes)
13. [Structure du projet](#structure-du-projet)
14. [Dépannage](#dépannage)
15. [Licence](#licence)

---

## Vue d'ensemble

```mermaid
flowchart TB
  subgraph api [APIs]
    OW[OpenWeather One Call 3.0]
    AQ[AQICN WAQI]
  end

  subgraph p1 [Phase 1 — Extraction]
    EX[Extract-first<br/>10 villes]
  end

  subgraph p2 [Phase 2 — Bronze]
    BR[(MinIO / disque<br/>JSON bruts + _storage)]
  end

  subgraph p3 [Phase 3 — Silver]
    W[WeatherTransformer]
    A[AirQualityTransformer]
    SW[(silver_weather)]
    SA[(silver_air_quality)]
  end

  subgraph p4 [Phase 4 — Gold]
    G[GoldPipeline]
    GW[(gold_weather_daily)]
    GA[(gold_air_quality_daily)]
    GC[(gold_daily)]
  end

  subgraph al [Alertes]
    I[Immédiat]
    D[Digest]
    O[Ops]
    AN[(alert_notifications)]
  end

  OW --> EX
  AQ --> EX
  EX --> BR
  BR --> W --> SW
  BR --> A --> SA
  SW --> G
  SA --> G
  G --> GW
  G --> GA
  G --> GC
  W --> I
  A --> I
  G --> D
  EX --> O
  I --> AN
  D --> AN
  O --> AN
```

| Couche | Contenu | Où c'est stocké |
|--------|---------|-----------------|
| **Bronze** | Réponse API **non modifiée** + métadonnées `_storage` | MinIO `raw/…` ou `data/raw/` |
| **Silver** | 1 document / ville / heure Paris, champs typés et validés | MongoDB |
| **Gold** | 1 document / ville / jour Paris, KPIs + confiance | MongoDB |
| **Alertes** | Historique d'envoi (anti-spam) | MongoDB `alert_notifications` |

**Planification** : `scheduler.py` exécute le pipeline à **minute 5** de chaque heure (`Europe/Paris`), plus un run au démarrage. Point d'entrée : `transformations/pipline_final.py`.

---

## Démarrage rapide

### Docker (recommandé)

```bash
git clone <url> weather_etl_v2
cd weather_etl_v2
cp .env.example .env
# Éditer .env : OPENWEATHER_API_KEY, AQICN_API_KEY, optionnel Discord
docker compose up -d --build
docker compose logs -f etl
```

### Local

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
python test_setup.py
python transformations/pipline_final.py
```

### Tester les alertes Discord

```bash
python scripts/test_alerts.py --ping
python scripts/test_alerts.py --type all --force
```

---

## Architecture du pipeline

Classe principale : **`WeatherETLPipeline`** dans `pipline_final.py`.

Principe **extract-first** : toutes les villes sont extraites (phase 1), puis tout le Bronze est écrit (phase 2), puis toutes les transformations Silver (phase 3), puis Gold (phase 4). Les clés MinIO sont **reliées** même si un run précédent a déjà écrit le Bronze (`_link_raw_keys_from_storage`).

| Phase | Méthode | Description |
|-------|---------|-------------|
| **1. Extraction** | `_extract_all_cities` | `OpenWeatherClient` + `AQICNClient` par ville |
| **2. Bronze** | `_save_all_raw` | `HivePartitionedStorage` → fichiers horaires |
| **2b. Liaison** | `_link_raw_keys_from_storage` | Retrouve les JSON raw sur MinIO/disque |
| **3. Silver** | `_transform_all` | Transformateurs → `insert_silver_*` MongoDB |
| **3b. Alertes** | `AlertService` | Immédiat après insert Silver réussi |
| **4. Gold** | `_run_gold` → `GoldPipeline.run()` | Agrégation du jour Paris en cours |
| **4b. Alertes** | `AlertService` | Digest + rollup ops en fin de run |

Résumé retourné par `run_hourly()` : `success_rate`, `mongodb_stats`, `api_breakdown`, etc.

---

## Couche Bronze (raw)

### Sources API

| API | Client | Endpoint / usage |
|-----|--------|------------------|
| **OpenWeatherMap** | `clients/openweather_client.py` | One Call 3.0 `/onecall` — données **horaires** (`hourly[]`) |
| **AQICN** | `clients/aqicn_client.py` | Feed WAQI par coordonnées — `data.aqi`, `data.iaqi`, `data.time`, … |

Retries HTTP : `utils/retry.py` (`MAX_RETRIES`, `REQUEST_TIMEOUT`).

### Écriture Bronze

`storage/hive_storage.py` :

- **Météo** : pour chaque entrée `hourly[]` dont l'heure Paris = heure de référence, crée un fichier avec un objet `hourly` **plat** (une heure par fichier).
- **Air** : enregistre la réponse AQICN complète pour l'heure de référence.

### Arborescence (partitionnement Hive)

```
raw/
└── city=paris/
    └── year=2026/
        └── month=05/
            └── day=20/
                ├── weather_14_raw.json
                └── air_quality_14_raw.json
```

`HH` = heure locale Paris extraite de la donnée.

### Bloc `_storage` (ajouté à chaque JSON Bronze)

| Champ | Rôle |
|-------|------|
| `city` | Nom de la ville |
| `filepath` | Clé objet MinIO / chemin relatif |
| `api_source` | `openweather` ou `aqicn` |
| `hour_timestamp_paris` | Début d'heure Paris (ISO) |
| `hour_timestamp_utc` | Même instant en UTC |
| `saved_at` | Horodatage d'écriture ETL |

Ce bloc est **critique** pour Silver : sans horodatage fiable, l'enregistrement est **rejeté**.

---

## Couche Silver (nettoyage)

Transformateurs : `improved_weather_transformer.py`, `improved_air_quality_transformer.py`.  
Utilitaires : `transformationscommon_cleaning.py`.

### Document MongoDB Silver

Enveloppe commune (`mongodb_storage.py`) :

| Champ racine | Description |
|--------------|-------------|
| `city` | Ville (minuscules) |
| `date_paris` | `YYYY-MM-DD` |
| `hour_paris` | 0–23 |
| `cleaned_data` | **Tout** le JSON transformé ci-dessous |
| `etl_timestamp` | Moment de l'insert MongoDB |

**Index unique** : `(city, date_paris, hour_paris)` — un rerun horaire **écrase** la même heure (upsert).

---

### Silver météo — traitements

**Entrée** : fichier Bronze avec `hourly` + `_storage`.

#### Horodatage

Ordre de priorité :

1. `_storage.hour_timestamp_paris` / `_utc` / `hour_timestamp`
2. `hourly.dt` (Unix → Paris)
3. `fallback_now` → **`ValueError`** (enregistrement refusé)

#### Validation physique (`VALID_RANGES`)

| Champ Silver | Source API (`hourly`) | Plage valide |
|--------------|----------------------|--------------|
| `temperature_celsius` | `temp` | −100 … 100 |
| `feels_like_celsius` | `feels_like` | idem (sinon reprend `temp`) |
| `humidity_percent` | `humidity` | 0 … 100 |
| `pressure_hpa` | `pressure` | 800 … 1100 |
| `dew_point_celsius` | `dew_point` | −100 … 50 |
| `wind_speed_mps` | `wind_speed` | 0 … 150 |
| `wind_gust_mps` | `wind_gust` | 0 … 150 |
| `wind_direction_deg` | `wind_deg` | 0 … 360 |
| `cloud_coverage_percent` | `clouds` | 0 … 100 |
| `uvi` | `uvi` | 0 … 20 |
| `visibility_m` | `visibility` | 0 … 100 000 |
| `precipitation_probability_percent` | `pop` | 0 … 100 (×100 si fraction ≤ 1) |

Hors plage → `null` + log `WARNING`.

#### Champs principaux Silver météo

| Champ | Description |
|-------|-------------|
| `timestamp_utc`, `timestamp_paris` | ISO |
| `hour`, `hour_paris`, `hour_formatted` | Ex. `14:00` |
| `weather_main`, `weather_description`, `weather_icon`, `weather_id` | Condition (`weather[0]`) |
| `wind_direction_cardinal` | N, NE, … (16 points) |
| `uvi_category` | `low` … `extreme` |
| `latitude`, `longitude`, `timezone` | Métadonnées lieu |

#### Champs dérivés (si entrées présentes)

| Champ | Règle |
|-------|--------|
| `heat_index_celsius` | Formule humidex simplifiée si temp ≥ 27 °C et humidité ≥ 40 % |
| `weather_severity` | Score combiné temp / vent(rafales) / UV → `normal`, `moderate`, `severe`, `extreme` |

Seuils sévérité (score cumulé) :

- Temp : ≤ −20 / ≥ 40 (+3), ≤ −10 / ≥ 35 (+2), ≤ 0 / ≥ 30 (+1)
- Vent max : ≥ 30 (+3), ≥ 20 (+2), ≥ 10 (+1)
- UV : ≥ 11 (+2), ≥ 8 (+1)
- Score ≥ 5 → `extreme`, ≥ 3 → `severe`, ≥ 1 → `moderate`

#### Métadonnées qualité

```json
"_data_quality": {
  "completeness_score": 0.8,
  "available_core_fields": 4,
  "total_core_fields": 5,
  "timestamp_source": "storage",
  "missing_core_fields": ["wind_speed_mps"]
},
"_lineage": {
  "transformer": "WeatherTransformer",
  "version": "3.0",
  "transformed_at": "...",
  "raw_source": "raw/city=paris/..."
}
```

---

### Silver qualité de l'air — traitements

**Entrée** : Bronze avec `data` (AQICN).

#### Horodatage

1. `_storage` (comme météo)
2. `data.time.v` (Unix)
3. `fallback_now` → **rejet**

#### AQI et alertes santé

| Champ | Traitement |
|-------|------------|
| `aqi` | Entier 0–500 ou **`null`** — **jamais** de défaut à `0` |
| `alert_level` | Uniquement si AQI présent (seuils EPA) |
| `aqi_category` | Libellé lisible dérivé de `alert_level` |
| `health_risk` | Objet si AQI + alerte : `score`, `outdoor_activity`, `mask_recommended`, … |

**Seuils `alert_level` (AQI)** :

| AQI | `alert_level` |
|-----|----------------|
| 0–50 | `good` |
| 51–100 | `moderate` |
| 101–150 | `unhealthy_sensitive` |
| 151–200 | `unhealthy` |
| 201–300 | `very_unhealthy` |
| 301+ | `hazardous` |

#### Polluants (`data.iaqi`)

| Champ Silver | Clé API | Plage validation (µg/m³ sauf CO) |
|--------------|---------|----------------------------------|
| `pm25` | `pm25.v` | 0 … 1000 |
| `pm10` | `pm10.v` | 0 … 1000 |
| `no2` | `no2.v` | 0 … 2000 |
| `o3` | `o3.v` | 0 … 1000 |
| `co` | `co.v` | 0 … 100 (mg/m³) |
| `so2` | `so2.v` | 0 … 2000 |
| `no`, `nh3` | `no.v`, `nh3.v` | 0 … 5000 |

**Cohérence** : si `pm25 > pm10` → `pm25` mis à `null` (log warning).

**Clés courtes météo dans `iaqi`** : `t` (temp), `h` (humidité), `p` (pression), `w` / `wg` (vent), `dew`.

#### Analyse

| Champ | Description |
|-------|-------------|
| `primary_pollutant` | Plus forte concentration parmi pm25, pm10, o3, no2, so2, co |
| `dominant_pollutant_api` | Champ API `dominentpol` |
| `pm25_to_pm10_ratio`, `no2_to_o3_ratio` | Si dénominateur > 0 |
| `uvi_forecast_daily` | Prévision UV depuis `forecast.daily.uvi` |
| `station_id`, `station_name`, `station_coordinates`, `attributions` | Métadonnées station |

`_data_quality` inclut `aqi_present`, `missing_pollutants`, `timestamp_source`.

---

### Politique des valeurs manquantes

| Situation | Comportement |
|-----------|--------------|
| Champ absent API | `null` en Silver |
| Valeur invalide / hors plage | `null` + log |
| Pas d'heure fiable | **Exception** — pas d'insert MongoDB |
| Dérivé sans entrées | `null` (pas de `0` artificiel) |
| Gold — moyennes | Calculées **uniquement** sur heures non nulles |
| Jour peu couvert | `is_trusted: false`, `coverage_pct` bas |

Constantes Gold (`transformationscommon_cleaning.py`) :

- `MIN_TRUSTED_HOURS = 18`
- `MIN_DATA_QUALITY_SCORE = 0.7`

---

## Couche Gold (agrégation)

Fichier : `improved_gold_pipeline.py`. Lecture Silver via `iter_silver_*_records(date_paris)` — par défaut le **jour Paris de l'heure de référence** du run.

### Pré-traitement

1. Regroupement par `(city, date_paris)`
2. **`dedupe_records_by_hour`** : 1 ligne / heure, dernière `_lineage.transformed_at` gagne
3. Tri par `hour_paris`

### Gold météo journalier (`gold_weather_daily`)

Champs principaux dans `analytics` :

| Catégorie | Champs |
|-----------|--------|
| **Identité** | `city`, `date`, `records_count`, `hours_covered`, `hourly_data[]` |
| **Température** | `avg_temperature`, `min_temperature`, `max_temperature`, `temp_range`, `temp_volatility`, `temp_trend` |
| **Autres** | `avg_humidity`, `avg_pressure`, `avg_wind_speed`, `max_wind_gust`, `max_uvi`, `avg_visibility`, … |
| **Conditions** | `dominant_weather_condition`, `max_severity`, `precipitation_detected` |
| **Confort** | `comfort_index` (`score`, `level`, …) — `null` si pas d'humidité |
| **Qualité** | `data_quality_score`, `hours_with_metric`, `hours_total`, `coverage_pct`, `is_trusted` |
| **Alerte métier** | `extreme_weather_flag` (temp extrême, vent ≥ 20 m/s, sévérité severe/extreme) |

`temp_trend` / tendances : comparaison moyenne première vs seconde moitié de la série horaire (`rising` / `falling` / `stable`).

### Gold air journalier (`gold_air_quality_daily`)

| Catégorie | Champs |
|-----------|--------|
| **AQI** | `avg_aqi`, `min_aqi`, `max_aqi`, `aqi_volatility`, `aqi_trend` |
| **Alertes** | `max_alert_level`, `alert_levels_distribution`, `unhealthy_hours_count`, `unhealthy_hours_percent` |
| **Polluants** | `avg_pm25`, `max_pm25`, … (no2, o3, co, so2) |
| **Santé** | `avg_health_risk_score`, `max_health_risk_score` |
| **Flags** | `significant_pollution_flag` (max AQI ≥ 150 **ou** ≥ 4 h avec AQI > 100) |
| **Confiance** | `is_trusted`, `coverage_pct`, … |

### Gold combiné (`gold_daily`)

Jointure villes/dates présentes dans **les deux** gold météo et air :

| Champ | Description |
|-------|-------------|
| `weather_comfort_score` | Depuis `comfort_index.score` |
| `air_quality_score` | `100 - avg_aqi/5` si AQI dispo |
| `outdoor_activity_score` | 40 % météo + 60 % air — `null` si donnée manquante |
| `outdoor_activity_recommendation` | `excellent` … `avoid` ou `insufficient_data` |
| `health_advisory` | Texte selon `max_aqi` |
| `weather_is_trusted`, `air_quality_is_trusted` | Indicateurs de couverture |

---

## Système d'alertes Discord

Module **`alerts/`** : `rules.py`, `service.py`, `discord.py`, `store.py`.

### Canaux et webhooks

| Canal | Variable `.env` | Fallback |
|-------|-----------------|----------|
| Immédiat | `DISCORD_WEBHOOK_IMMEDIATE` | `DISCORD_WEBHOOK_URL` |
| Digest | `DISCORD_WEBHOOK_DIGEST` | `DISCORD_WEBHOOK_URL` |
| Ops | `DISCORD_WEBHOOK_OPS` | `DISCORD_WEBHOOK_URL` |

Si **aucune** URL n'est définie : pipeline inchangé, log `Discord alerts disabled`.

### Règles de déclenchement

#### Immédiat (après insert Silver)

| Type | Condition |
|------|-----------|
| **Météo** | `weather_severity` ∈ `severe`, `extreme` |
| **Air** | `alert_level` ∈ `unhealthy_sensitive`, `unhealthy`, `very_unhealthy`, `hazardous` **et** `aqi_present` |

Embed Discord : ville, heure Paris, métriques clés, gravité `warning` ou `critical`.

#### Digest (après Gold, 1× / jour Paris)

Message si au moins une ville a :

- `extreme_weather_flag` **et** `is_trusted`, et/ou
- `significant_pollution_flag` **et** `is_trusted`

Clé déduplication : `digest:{date_paris}`.

#### Ops (fin de `run_hourly`)

Message si :

- Erreur extract / pas de données ville
- Échec transform Silver (`ETLResult.success == false`)
- Échec agrégation Gold

Clé : `ops:{reference_hour_iso}`.

### Déduplication

Collection MongoDB **`alert_notifications`** :

- Index unique sur `alert_key`
- Exemples : `immediate:weather:paris:2026-05-20:14:severity:extreme`, `digest:2026-05-20`, `ops:2026-05-20T12:00:00+02:00`

Pour retester : `python scripts/test_alerts.py --force` ou supprimer des clés en base.

### Tester les notifications

```bash
# 1. Ping webhook
python scripts/test_alerts.py --ping

# 2. Simuler tous les types (données fictives)
python scripts/test_alerts.py --type all --force

# 3. Un type précis
python scripts/test_alerts.py --type weather --force
python scripts/test_alerts.py --type aqi --force
python scripts/test_alerts.py --type digest --force
python scripts/test_alerts.py --type ops --force

# Sans MongoDB (pas de déduplication persistée)
python scripts/test_alerts.py --ping --no-mongo
```

Créer un webhook : Discord → Salon → **Modifier le salon** → **Intégrations** → **Webhooks** → **Nouveau webhook** → copier l'URL dans `.env`.

---

## Configuration (.env)

Copier `.env.example` vers `.env`.

### API (obligatoire)

| Variable | Description |
|----------|-------------|
| `OPENWEATHER_API_KEY` | [OpenWeatherMap](https://openweathermap.org/api) |
| `AQICN_API_KEY` | [AQICN](https://aqicn.org/data-platform/token/) |

### Stockage

| Variable | Défaut | Description |
|----------|--------|-------------|
| `STORAGE_BACKEND` | `local` dans code / `minio` dans Docker | `local` ou `minio` |
| `DATA_BASE_PATH` | `./data` | Racine données mode local |
| `DATA_RAW_PATH` | `{DATA_BASE_PATH}/raw` | Bronze local |
| `MINIO_ENDPOINT` | `localhost:9000` | Hôte:port (Docker : `minio:9000`) |
| `MINIO_ACCESS_KEY` | `minioadmin` | |
| `MINIO_SECRET_KEY` | `minioadmin` | |
| `MINIO_BUCKET` | `weather-etl` | |
| `MINIO_SECURE` | `false` | `true` si HTTPS |

### MongoDB

| Variable | Défaut Docker hôte | Description |
|----------|-------------------|-------------|
| `MONGODB_HOST` | `localhost` | |
| `MONGODB_PORT` | `27018` (hôte) / `27017` (conteneur) | |
| `MONGODB_DATABASE` | `weather_etl` | |
| `MONGODB_USERNAME` | `weather_user` | |
| `MONGODB_PASSWORD` | `weather_pass` | |
| `MONGODB_AUTH_SOURCE` | `weather_etl` | |
| `MONGODB_URI` | — | Optionnel, remplace host/port/user |

**Compass** :

```
mongodb://weather_user:weather_pass@localhost:27018/weather_etl?authSource=weather_etl
```

### Logs & HTTP

| Variable | Défaut | Description |
|----------|--------|-------------|
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `LOG_PATH` | `data/logs` | Fichiers planificateur |
| `REQUEST_TIMEOUT` | `30` | Secondes |
| `MAX_RETRIES` | `3` | Retries API |
| `LOCAL_TIMEZONE` | `Europe/Paris` | Alignement horaire (documentation) |

### Discord (optionnel)

| Variable | Usage |
|----------|--------|
| `DISCORD_WEBHOOK_URL` | Webhook par défaut (tous canaux) |
| `DISCORD_WEBHOOK_IMMEDIATE` | Alertes horaires Silver |
| `DISCORD_WEBHOOK_DIGEST` | Résumé journalier Gold |
| `DISCORD_WEBHOOK_OPS` | Échecs pipeline |

---

## Guide d'utilisation

### Production — planificateur horaire

```bash
python scheduler.py
```

Docker : service **`etl`** → `python scheduler.py`, logs volume **`etl_logs`**.

### Pipeline complet (une fois)

```bash
python transformations/pipline_final.py
```

### Bronze seul (sans Silver/Gold)

```bash
python fetch_data.py
python fetch_data.py --weather
python fetch_data.py --air-quality
python fetch_data.py --town paris --hours 3
python fetch_data.py --list-towns
```

Basé sur `etl/pipeline.py` (legacy).

### Rattrapage Mongo depuis MinIO

Si le Bronze existe mais MongoDB est vide :

```bash
python scripts/backfill_mongo_from_minio.py
```

### Vérification installation

```bash
python test_setup.py
```

### API Python

```python
from transformations.pipline_final import run_hourly_etl_job

summary = run_hourly_etl_job(hours_back=1)
print(summary["success_rate"])
print(summary.get("mongodb_stats"))
```

### Services Docker Compose

| Service | Port / accès | Rôle |
|---------|--------------|------|
| `etl` | logs `docker compose logs etl` | Scheduler + pipeline |
| `minio` | 9000 API, 9001 console | Bronze |
| `mongodb` | 27018 → 27017 | Silver, Gold, alertes |
| `jupyter` | 8888 | Notebooks ML |
| `minio-init` | — | Crée le bucket `weather-etl` |

---

## Stockage MongoDB & MinIO

### Collections MongoDB

| Collection | Clé logique | Contenu |
|------------|-------------|---------|
| `silver_weather` | ville + date + heure | `cleaned_data` météo |
| `silver_air_quality` | ville + date + heure | `cleaned_data` air |
| `gold_weather_daily` | ville + `date` | `analytics` météo jour |
| `gold_air_quality_daily` | ville + `date` | `analytics` air jour |
| `gold_daily` | ville + `date` | `analytics` combiné |
| `alert_notifications` | `alert_key` unique | Anti-doublon Discord |

### Requêtes utiles (Compass / mongosh)

```javascript
// Dernière heure météo Paris
db.silver_weather.find({ city: "paris" }).sort({ hour_paris: -1 }).limit(1)

// Gold air du jour avec pollution significative
db.gold_air_quality_daily.find({
  "analytics.significant_pollution_flag": true
})

// Alertes déjà envoyées aujourd'hui
db.alert_notifications.find({ alert_key: /^digest:/ })
```

---

## Machine Learning

Répertoire `MachineLearning/` — indépendant des alertes Discord.

| Script | Rôle |
|--------|------|
| `data_pipeline.py` | Silver/Gold → CSV features |
| `train_aqi.py` | Random Forest sur AQI |
| `predict.py` | Inférence |
| `anomaly_detection.py` | Écarts modèle vs AQI réel |
| `mongo_loader.py` | Chargement direct depuis MongoDB |

```bash
pip install -r requirements-ml.txt
python -m MachineLearning.data_pipeline
python -m MachineLearning.train_aqi
python -m MachineLearning.predict
python -m MachineLearning.anomaly_detection
```

Artefacts :

- `MachineLearning/data/gold/dataset_gold.csv`
- `MachineLearning/models/aqi_model.pkl`
- Notebook : `MachineLearning/notebooks/01_mongo_ml_workflow.ipynb`

---

## Villes couvertes

Définies dans `config/towns.py` (`FRENCH_TOWNS`) :

| Ville | `name` | Lat | Lon |
|-------|--------|-----|-----|
| Paris | `paris` | 48.8566 | 2.3522 |
| Marseille | `marseille` | 43.2965 | 5.3698 |
| Lyon | `lyon` | 45.7640 | 4.8357 |
| Toulouse | `toulouse` | 43.6047 | 1.4442 |
| Nice | `nice` | 43.7102 | 7.2620 |
| Nantes | `nantes` | 47.2184 | -1.5536 |
| Montpellier | `montpellier` | 43.6108 | 3.8767 |
| Strasbourg | `strasbourg` | 48.5734 | 7.7521 |
| Bordeaux | `bordeaux` | 44.8378 | -0.5792 |
| Lille | `lille` | 50.6292 | 3.0573 |

---

## Structure du projet

```
weather_etl_v2/
├── alerts/                      # Notifications Discord
│   ├── service.py               # Orchestration
│   ├── rules.py                 # Seuils & messages
│   ├── discord.py               # Webhook HTTP
│   ├── store.py                 # Déduplication MongoDB
│   └── models.py
├── clients/                     # Clients API
├── config/
│   ├── settings.py              # Variables .env
│   └── towns.py
├── transformations/
│   ├── pipline_final.py         # Pipeline principal
│   ├── improved_weather_transformer.py
│   ├── improved_air_quality_transformer.py
│   ├── improved_gold_pipeline.py
│   └── transformationscommon_cleaning.py
├── storage/
│   ├── data_store.py            # Local / MinIO
│   ├── hive_storage.py          # Bronze partitionné
│   └── mongodb_storage.py
├── MachineLearning/
├── scripts/
│   ├── backfill_mongo_from_minio.py
│   └── test_alerts.py           # Tests alertes Discord
├── utils/
│   ├── dedupe.py                # Dédup heures Gold
│   ├── timezone_utils.py        # Europe/Paris
│   ├── logger.py
│   └── retry.py
├── etl/pipeline.py              # Bronze legacy (fetch_data)
├── scheduler.py                 # Cron horaire
├── fetch_data.py
├── test_setup.py
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── requirements-ml.txt
├── .env.example
└── README.md
```

---

## Dépannage

| Symptôme | Piste |
|----------|--------|
| Silver vide / transform fail | Vérifier `_storage.hour_timestamp_*` dans Bronze ; logs `Unreliable timestamp` |
| Gold `is_trusted: false` | Moins de 18 h de données ou `data_quality_score` < 0,7 |
| Pas d'alerte Discord | `DISCORD_WEBHOOK_*` dans `.env` ; redémarrer `etl` ; `python scripts/test_alerts.py --ping` |
| Alerte une seule fois | Déduplication normale — `--force` pour test ou supprimer `alert_notifications` |
| MongoDB connexion refusée | Port 27018 (Docker) vs 27017 (local) ; identifiants `weather_user` |
| MinIO bucket manquant | `docker compose up minio-init` ou créer `weather-etl` à la main |
| AQI toujours `null` | Réponse AQICN sans `data.aqi` ; pas de faux 0 |
| OpenWeather 401 | Clé API invalide ou plan One Call 3.0 requis |

Logs détaillés : `LOG_LEVEL=DEBUG` dans `.env`.

---

## Licence

MIT
