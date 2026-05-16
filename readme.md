# 📊 Pipeline de Données de Bout en Bout : Ingestion API YouTube & Orchestration ELT

Architecture de données moderne pour extraire, charger et transformer des statistiques de chaînes YouTube. L'infrastructure est entièrement conteneurisée avec **Docker** et orchestrée par **Apache Airflow**. Les données transitent par un Data Warehouse **PostgreSQL** structuré en deux couches (*Staging* et *Core*) pour des analyses décisionnelles fiables.

---

## 🎯 Compétences couvertes

| Domaine | Détail |
|---|---|
| **Conteneurisation** | Images Docker personnalisées pour le Data Engineering |
| **Orchestration** | Apache Airflow — DAGs, dépendances, scheduling |
| **Modélisation** | Data Warehouse PostgreSQL en couches Staging / Core |
| **SQL & Python** | `psycopg2`, `PostgresHook`, upsert, conversion ISO 8601 |
| **Sécurité** | Gestion des secrets via fichier `.env` non versionné |

---

## 🏗️ Architecture Technique

Infrastructure multi-conteneurs via **Docker Compose** avec l'exécuteur **CeleryExecutor** d'Airflow :

```
               +-------------------------+
               |  Fichier .env (Sécurisé)|
               +-----------+-------------+
                           |
                           v
           +--------------------------------+
           |         Docker Compose         |
           |                                |
           |  +--------------------------+  |
           |  |      Apache Airflow      |  |
           |  |  - Webserver (UI)        |  |
+-------+  |  |  - Scheduler             |  |  +----------------+
|  API  |  |  |  - Worker (Celery)       |  |  | Data Warehouse |
|  YT   |->|  +------------+-------------+  |->|   PostgreSQL   |
+-------+  |               |                |  |  - Staging     |
           |               v                |  |  - Core        |
           |  +--------------------------+  |  +----------------+
           |  |     Redis (Broker)       |  |          ^
           |  +--------------------------+  |          |
           +--------------------------------+   DBeaver / BI
```

### Composants du cluster

| Composant | Rôle |
|---|---|
| **Airflow Webserver** | Interface graphique pour activer, suivre et inspecter les DAGs |
| **Airflow Scheduler** | Orchestre le flux de tâches selon les dépendances |
| **Airflow Worker** | Exécute concrètement les tâches Python du pipeline |
| **Redis** | Broker de messages entre Scheduler et Workers |
| **PostgreSQL (Metadata)** | Stocke l'état interne de l'application Airflow |
| **PostgreSQL (Data Warehouse)** | Base cible avec les schémas `staging` et `core` |

---

## 📁 Structure du Projet

```text
.
├── dags/                        # Définitions des DAGs Airflow
│   └── youtube_elt_pipeline.py
├── logs/                        # Logs d'exécution (générés automatiquement)
├── data/                        # Fichiers de données temporaires ou locaux
├── include/                     # Scripts SQL réutilisables
│   └── sql/
│       ├── creation_tables.sql
│       └── transformations.sql
├── tests/                       # Tests unitaires des scripts d'extraction
├── .env                         # Variables d'environnement (non versionné)
├── .gitignore
├── Dockerfile                   # Image Airflow personnalisée
├── requirements.txt             # Dépendances Python
└── docker-compose.yml           # Orchestration des conteneurs
```

---

## 🪜 Étapes du Pipeline

### 1. Ingestion → Staging

Connexion à l'**API YouTube Data v3** pour extraire les statistiques brutes (vues, likes, commentaires, durées, métadonnées). Les données sont stockées telles quelles dans la couche `staging` pour garantir l'idempotence du pipeline.

### 2. Transformation ELT

Les transformations s'opèrent directement dans PostgreSQL :

- **Normalisation temporelle** — Conversion du format ISO 8601 (ex: `PT15M30S`) en `interval` PostgreSQL exploitable.
- **Logique métier** — Segmentation automatique des vidéos par durée : `video_type` = `Shorts` ou `Vidéo Standard`.

### 3. Consolidation → Core (Upsert)

Stratégie **`INSERT ... ON CONFLICT DO UPDATE`** pour éviter les doublons tout en maintenant les compteurs dynamiques à jour (vues, likes). Les données obsolètes issues de data drift sont nettoyées.

---

## 🚀 Déploiement

### Prérequis

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Une clé API **YouTube Data v3** ([Google Cloud Console](https://console.cloud.google.com/))
- [DBeaver](https://dbeaver.io/) ou tout autre client SQL

### Étape 1 — Configurer l'environnement

Créez un fichier `.env` à la racine du projet :

```env
# Airflow
AIRFLOW_IMAGE_NAME=apache/airflow:2.7.1-python3.10
AIRFLOW__CORE__LOAD_EXAMPLES=false

# API YouTube
YOUTUBE_API_KEY=your_youtube_api_key_here   # 🔑 Google Cloud Console
# PostgreSQL Data Warehouse
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=youtube_analytics
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

```

### Étape 2 — Initialiser et démarrer

```bash
# Initialiser la base de données Airflow
docker-compose up airflow-init

# Démarrer tous les services en arrière-plan
docker-compose up -d

# Vérifier l'état des conteneurs
docker-compose ps
```

### Étape 3 — Accéder aux interfaces

| Interface | URL / Paramètre | Identifiants |
|---|---|---|
| **Airflow UI** | http://localhost:8080 | `airflow` / `airflow` |
| **DBeaver — Hôte** | `localhost:5432` | Valeurs du `.env` |
| **DBeaver — Base** | `youtube_analytics` | — |

---

## 🔄 Workflow DAG

Le DAG s'exécute selon l'ordonnancement séquentiel suivant :

```
[Vérification_API]
       ↓
[Extraction_YouTube]
       ↓
[Chargement_Staging]
       ↓
[Transformations_ELT]
       ↓
[Upsert_Core_Data]
```

Chaque tâche utilise le `PostgresHook` d'Airflow et le driver `psycopg2` pour une gestion propre des sessions et curseurs de base de données.

---

## 🛠️ Maintenance & Debugging

```bash
# Suivre les logs d'un conteneur en temps réel
docker-compose logs -f airflow-worker

# Arrêter proprement (données conservées)
docker-compose down

# Remise à zéro complète (supprime les volumes)
docker-compose down -v
```"# ELT-Pipeline-Industrialis-YouTube-API-Docker-Airflow" 
"# ELT_pipeline_youtube_docker_airflow" 
