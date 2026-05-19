# Pipeline ETL météo & qualité de l'air

Pipeline ETL horaire qui collecte les données météorologiques (**OpenWeatherMap**) et de qualité de l'air (**AQICN**) pour les **10 plus grandes villes de France**, les stocke selon une architecture **medallion** (Bronze / Silver / Gold) et alimente un module **Machine Learning** pour la prédiction et la détection d'anomalies sur l'indice AQI.

## Vue d'ensemble

```mermaid
flowchart LR
  subgraph sources [Sources]
    OW[OpenWeatherMap]
    AQ[AQICN]
  end

  subgraph etl [ETL horaire]
    EX[Extraction]
    TR[Transformation]
    AG[Agrégation journalière]
  end

  subgraph stockage [Stockage]
    BR[(Bronze — MinIO / disque local)]
    SI[(Silver — MongoDB)]
    GO[(Gold — MongoDB)]
  end

  subgraph ml [Machine Learning]
    NB[Jupyter Lab]
    TRN[Entraînement AQI]
    PRD[Prédiction & anomalies]
  end

  OW --> EX
  AQ --> EX
  EX --> BR
  EX --> TR --> SI
  SI --> AG --> GO
  GO --> TRN
  GO --> PRD
  SI --> NB
  GO --> NB
```

| Couche | Contenu | Backend par défaut (Docker) |
|--------|---------|-----------------------------|
| **Bronze** | Réponses brutes des API, partitionnées type Hive | MinIO (`raw/…`) |
| **Silver** | Données nettoyées et standardisées | MongoDB (`silver_weather`, `silver_air_quality`) |
| **Gold** | Agrégats journaliers par ville | MongoDB (`gold_weather_daily`, `gold_air_quality_daily`, `gold_daily`) |

Le planificateur (`scheduler.py`) exécute le pipeline complet à **:05 de chaque heure** (fuseau `Europe/Paris`), avec un premier lancement au démarrage.

## Structure du projet

```
weather_etl_v2/
├── config/
│   ├── settings.py              # Configuration (.env)
│   └── towns.py                 # 10 villes françaises + coordonnées
├── clients/
│   ├── openweather_client.py
│   └── aqicn_client.py
├── etl/
│   └── pipeline.py              # ETL Bronze uniquement (legacy / fetch_data)
├── transformations/
│   ├── pipline_final.py         # Pipeline complet Bronze → Silver → Gold
│   ├── improved_weather_transformer.py
│   ├── improved_air_quality_transformer.py
│   └── improved_gold_pipeline.py
├── storage/
│   ├── data_store.py            # Abstraction local / MinIO
│   ├── hive_storage.py          # Partitionnement Hive
│   └── mongodb_storage.py       # Couches Silver & Gold
├── MachineLearning/
│   ├── data_pipeline.py         # Jeu de données Gold pour le ML
│   ├── train_aqi.py             # Entraînement du modèle AQI
│   ├── predict.py               # Prédiction AQI
│   ├── anomaly_detection.py     # Détection d'anomalies
│   ├── mongo_loader.py
│   └── notebooks/               # Notebooks Jupyter
├── scripts/
│   └── backfill_mongo_from_minio.py  # Rejouer Silver/Gold depuis le Bronze
├── docker/
│   ├── Dockerfile.jupyter
│   ├── entrypoint.sh
│   └── mongo-init.js
├── utils/                       # Logger, retry, fuseau horaire, déduplication
├── scheduler.py                 # Planificateur APScheduler (point d'entrée prod)
├── fetch_data.py                # CLI extraction Bronze (rapide)
├── test_setup.py                # Vérification de l'installation
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── requirements-ml.txt
└── .env.example
```

## Prérequis

- **Python 3.12+**
- Clés API :
  - [OpenWeatherMap](https://openweathermap.org/api)
  - [AQICN](https://aqicn.org/data-platform/token/)
- Pour l'exécution conteneurisée : **Docker** et **Docker Compose**
- Optionnel en local : **MinIO**, **MongoDB** (ou via Docker Compose)

## Installation

### Option A — Docker Compose (recommandé)

1. Cloner le dépôt et se placer à la racine :

```bash
git clone <url-du-depot> weather_etl_v2
cd weather_etl_v2
```

2. Configurer l'environnement :

```bash
cp .env.example .env
# Renseigner OPENWEATHER_API_KEY et AQICN_API_KEY dans .env
```

3. Démarrer la stack :

```bash
docker compose up -d --build
```

Services exposés :

| Service | URL / port | Rôle |
|---------|------------|------|
| **etl** | — | Planificateur ETL (logs dans le volume `etl_logs`) |
| **MinIO** | http://localhost:9000 (API), :9001 (console) | Stockage Bronze (`minioadmin` / `minioadmin`) |
| **MongoDB** | `localhost:27018` | Silver & Gold (`weather_user` / `weather_pass`) |
| **Jupyter Lab** | http://localhost:8888 | Notebooks ML sous `MachineLearning/` |

> Sur Windows, le port hôte **27018** évite le conflit avec un `mongod` local sur `27017`.

4. Vérifier les logs ETL :

```bash
docker compose logs -f etl
```

### Option B — Exécution locale

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux / macOS
source venv/bin/activate

pip install -r requirements.txt
cp .env.example .env
# Éditer .env (clés API, STORAGE_BACKEND=local ou minio, MongoDB, etc.)
```

Pour le module ML et Jupyter en local :

```bash
pip install -r requirements-ml.txt
```

Lancer MinIO et MongoDB séparément, ou pointer `STORAGE_BACKEND=local` et ignorer MongoDB (Silver/Gold désactivés si la connexion échoue).

## Configuration

Variables principales (voir `.env.example` pour la liste complète) :

| Variable | Obligatoire | Défaut | Description |
|----------|:-----------:|--------|-------------|
| `OPENWEATHER_API_KEY` | Oui | — | Clé API OpenWeatherMap |
| `AQICN_API_KEY` | Oui | — | Jeton AQICN |
| `STORAGE_BACKEND` | Non | `minio` | `local` ou `minio` |
| `DATA_BASE_PATH` | Non | `./data` | Racine des données en mode `local` |
| `MINIO_ENDPOINT` | Si MinIO | `localhost:9000` | Hôte:port MinIO |
| `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` | Si MinIO | `minioadmin` | Identifiants MinIO |
| `MINIO_BUCKET` | Non | `weather-etl` | Nom du bucket |
| `MONGODB_HOST` | Non | `localhost` | Hôte MongoDB |
| `MONGODB_PORT` | Non | `27018` (hôte Docker) | Port MongoDB |
| `MONGODB_DATABASE` | Non | `weather_etl` | Base de données |
| `MONGODB_USERNAME` / `MONGODB_PASSWORD` | Non | `weather_user` / `weather_pass` | Utilisateur applicatif |
| `LOCAL_TIMEZONE` | Non | `Europe/Paris` | Alignement horaire des villes |
| `LOG_LEVEL` | Non | `INFO` | Niveau de log |
| `REQUEST_TIMEOUT` | Non | `30` | Timeout HTTP (secondes) |
| `MAX_RETRIES` | Non | `3` | Tentatives max sur les appels API |

## Utilisation

### Planificateur (production)

Pipeline complet : extraction → Bronze → Silver → Gold.

```bash
python scheduler.py
```

Sous Docker, le service `etl` exécute déjà cette commande.

### Exécution ponctuelle du pipeline complet

```bash
python transformations/pipline_final.py
```

### Extraction Bronze uniquement (CLI)

Utile pour tester les API sans passer par MongoDB :

```bash
python fetch_data.py                      # Météo + qualité de l'air, toutes les villes
python fetch_data.py --weather            # Météo seulement
python fetch_data.py --air-quality        # Qualité de l'air seulement
python fetch_data.py --town paris         # Une ville
python fetch_data.py --hours 3            # 3 dernières heures de météo
python fetch_data.py --list-towns         # Lister les villes
```

> `fetch_data.py` s'appuie sur `etl/pipeline.py` (couche Bronze). Pour Silver et Gold, utiliser le planificateur ou `pipline_final.py`.

### Rattrapage MongoDB depuis le Bronze

Si MinIO contient déjà les fichiers `raw` mais que MongoDB est vide ou incomplet :

```bash
python scripts/backfill_mongo_from_minio.py
```

### Vérifier l'installation

```bash
python test_setup.py
```

### Utilisation en bibliothèque

```python
from transformations.pipline_final import run_hourly_etl_job
from config.towns import FRENCH_TOWNS

summary = run_hourly_etl_job(hours_back=1)
print(summary["success_rate"], summary.get("mongodb_stats"))
```

## Stockage des données

### Bronze — partitionnement Hive

```
raw/
└── city=paris/
    └── year=2026/
        └── month=03/
            └── day=04/
                ├── weather_14_raw.json
                └── air_quality_14_raw.json
```

Convention de nommage :

- Météo : `weather_{HH}_raw.json`
- Qualité de l'air : `air_quality_{HH}_raw.json`

L'heure (`HH`) est dérivée de l'horodatage de la réponse API, alignée sur `Europe/Paris`.

### Silver & Gold — collections MongoDB

| Collection | Description |
|------------|-------------|
| `silver_weather` | Météo nettoyée (par ville / heure) |
| `silver_air_quality` | Qualité de l'air nettoyée |
| `gold_weather_daily` | Statistiques météo journalières |
| `gold_air_quality_daily` | Statistiques AQI journalières |
| `gold_daily` | Vue combinée météo + air par jour |

Connexion depuis **MongoDB Compass** (stack Docker) :

```
mongodb://weather_user:weather_pass@localhost:27018/weather_etl?authSource=weather_etl
```

## Machine Learning

Le dossier `MachineLearning/` construit un jeu d'entraînement à partir des données Gold (MongoDB ou CSV exporté), entraîne un **Random Forest** pour prédire l'AQI et détecte les écarts par rapport au modèle.

```bash
# Construire / mettre à jour le dataset Gold (CSV)
python -m MachineLearning.data_pipeline

# Entraîner le modèle (nécessite suffisamment de lignes Gold)
python -m MachineLearning.train_aqi

# Prédire un AQI à partir de features météo
python -m MachineLearning.predict

# Détecter les anomalies AQI
python -m MachineLearning.anomaly_detection
```

Artefacts générés :

- `MachineLearning/data/gold/dataset_gold.csv` — dataset d'entraînement
- `MachineLearning/models/aqi_model.pkl` — modèle sauvegardé

Workflow interactif : notebook `MachineLearning/notebooks/01_mongo_ml_workflow.ipynb` via Jupyter (`docker compose up jupyter` ou installation locale de `requirements-ml.txt`).

## Villes couvertes

| Ville | Latitude | Longitude |
|-------|----------|-----------|
| Paris | 48.8566 | 2.3522 |
| Marseille | 43.2965 | 5.3698 |
| Lyon | 45.7640 | 4.8357 |
| Toulouse | 43.6047 | 1.4442 |
| Nice | 43.7102 | 7.2620 |
| Nantes | 47.2184 | -1.5536 |
| Montpellier | 43.6108 | 3.8767 |
| Strasbourg | 48.5734 | 7.7521 |
| Bordeaux | 44.8378 | -0.5792 |
| Lille | 50.6292 | 3.0573 |

## Planification

- **Fréquence** : toutes les heures à **minute 5** (`Europe/Paris`)
- **Comportement** : un job au démarrage, puis exécution cron ; une seule instance à la fois (`max_instances=1`)
- **Flux** : extraction de toutes les villes → écriture Bronze → transformation Silver → agrégation Gold

Les logs du planificateur sont écrits dans `data/logs/` (ou le volume Docker `etl_logs`).

## Licence

MIT
