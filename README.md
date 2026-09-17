# multitudcsd
 
Motor de ingesta y procesamiento de datos de movilidad y afluencia durante el **CSD (Christopher
Street Day / Orgullo) de Berlín**, construido como **data lakehouse Medallion** sobre **Delta Lake**
y **PySpark**.
 
Trabajo Fin de Máster · Máster en Big Data & Data Engineering (UCM / NTIC)
Autor: Luis Javier Blanco · Tutores: Jorge Centeno y Alberto González
 
---

## Propósito

El proyecto ingiere datos de transporte público y tráfico de Berlín, los limpia y los geolocaliza en
celdas H3. Después los agrega en tablas de servicio que cruzan la presión sobre el transporte con la
actividad social durante el evento. Al final se entrena un modelo que ofrece una predicción del retaso
por línea.

---

```
Fuentes → Ingesta → BRONZE (crudo) → SILVER (limpio + H3) → GOLD (servicio)
                                                             └→ modelo servido como tabla Gold
```

---

El pipeline está dividido en dos etapas:

| Etapa | Objetivo | Datos |
|---|---|---|
| **1 · Transporte** | Retrasos en vivo, oferta y capacidad programada, disponibilidad de bici, cortes de tráfico | VBB GTFS-RT y GTFS estático, Nextbike GBFS, VIZ Berlín — **reales** |
| **2 · Social** | Señal de afluencia geolocalizada, cruzada con la presión de transporte | Menciones geolocalizadas — **sintéticas**, generadas por este repositorio |

---

## Requisitos

- **Python 3.11**
- **Java 17** (Temurin recomendado), necesario para Spark
- En **Windows**: `winutils.exe` y `hadoop.dll` (Hadoop 3.3.x) en `C:\Hadoop\bin`, o la ruta que se
  indique en `HADOOP_HOME`. `config.prepare_windows_hadoop()` lo comprueba al arrancar la sesión y
  falla con un mensaje explícito si faltan.

> **Git Bash**: no uses `source .venv/Scripts/activate`. Reescribe el `PATH` en formato Windows y
> Spark falla con `JAVA_GATEWAY_EXITED` porque la JVM no encuentra `cmd.exe`. Invoca el intérprete
> directamente: `.venv/Scripts/python.exe -m multitudcsd...`.

---

### Configuración

```bash
cp .env.example .env
```

| Variable | Valores | Para qué                                                                             |
|---|---|--------------------------------------------------------------------------------------|
| `ENV` | `local` \| `databricks` | Decide cómo se construye la `SparkSession` y dónde vive el lakehouse                 |
| `LAKEHOUSE_ROOT` | ruta | Raíz de las tablas Delta. En local admite ruta relativa al repo                      |
| `VBB_GTFS_RT_URL` | URL | Feed GTFS-Realtime de VBB                                                            |
| `NEXTBIKE_GBFS_DISCOVERY_URL` | URL | Índice GBFS de Nextbike, del que se escogen `station_status` y `station_information` |
| `VIZ_DISRUPTIONS_URL` | URL | Feed de obras y cortes de VIZ Berlín                                                 |

`config.py` es la única fuente de rutas, sesiones de Spark y parámetros del evento
(`FECHA_REFERENCIA`, `PUNTOS_DEL_RECORRIDO`, `MARGEN_KILOMETROS`). Cambiando esas constantes el motor sirve para otro evento y otra ciudad.

---

## Tests y calidad
 
```bash
make test    # pytest
make lint    # ruff
```

## Ejecución en local

```bash
# 1. El GTFS estatico es una foto completa y no entra en run_bronze: se descarga una vez
make ingest-gtfs-static

# 2. Pipeline completo: bronze -> stream (etapa 2) -> silver -> gold -> ml
make all
```
## Despliegue en Azure Databricks
Pasos del pase:
En el cluster, en *Advanced options → Environment variables*:
 
```
ENV=databricks
LAKEHOUSE_ROOT=/Volumes/<catalogo>/<esquema>/<volumen>/lakehouse
```

El lakehouse esta en un **Volume de Unity Catalog** porque admite entrada/salida de ficheros con
Python normal que necesita joblib.dump y el generador sintético.

```bash
make test && make lint     # todo verde en local
make build                 # dist/multitudcsd-0.1.1-py3-none-any.whl
```

Se sube el wheel al Volume de artefactos y se instala en cada tarea del Job. El wheel expone cinco
puntos de entrada, uno por etapa del pipeline:
 
| Entry point | Módulo |
|---|---|
| `multitudcsd-bronze` | `orchestration.run_bronze:main` |
| `multitudcsd-stream` | `orchestration.run_stream:main` |
| `multitudcsd-silver` | `orchestration.run_silver:main` |
| `multitudcsd-gold` | `orchestration.run_gold:main` |
| `multitudcsd-ml` | `orchestration.run_ml:main` |
 
La orquestación en la nube es **Databricks Jobs**, con la ingesta real y la etapa 2 en paralelo:
 
```
ingest_bronze ────┐
                  ├──> build_silver ──> build_gold ──> serve_predictions
stream_mentions ──┘
```

## Estructura del repositorio
 
```
multitudcsd/
├── app/streamlit_app.py            # explorador de tablas Gold
├── data/                           # gitignored: lakehouse, landing, _checkpoints, models
├── docs/
│   ├── decisiones.md               # fecha · decisión · alternativas · motivo
│   └── kafka_demo/                 # demo de la alternativa descartada, fuera del pipeline
├── notebooks/                      # exploración. 
├── src/multitudcsd/
│   ├── config.py                   # entorno, rutas, SparkSession y parámetros del evento
│   ├── storage.py                  # única puerta de lectura/escritura Delta, batch y streaming
│   ├── ingestion/                  # → BRONZE: http_request, gtfs_rt, gtfs_static, gbfs, viz
│   ├── transforms/                 # → SILVER y GOLD: geo (H3), bronze_to_silver, silver_to_gold
│   ├── synthetic/mentions.py       # etapa 2: generador reproducible → landing/
│   ├── streaming/mentions_stream.py# etapa 2: file source → bronze_csd_mentions
│   ├── ml/                         # features, train, predict
│   └── orchestration/              # run_bronze, run_stream, run_silver, run_gold, run_ml, run_all
└── tests/                          # fixtures + suite completa, sin acceso a red
```
 
Reglas por carpeta:
 
- `config.py` es la única fuente de rutas y sesiones. 
- `storage.py` es la única puerta de escritura Delta, también en streaming.
- Todo acceso HTTP externo pasa por `ingestion/http_request.py`, que centraliza reintentos y el
  `User-Agent` (VBB rechaza las peticiones que no lo llevan).
---

## Licencia 
El código se publica bajo licencia MIT (ver `LICENSE`)

los datos proceden de fuentes abiertas de terceros con sus propias licencias, que no están
alteradas por la anterior:

- VBB GTFS estático y GTFS-Realtime — CC BY 4.0 — © VBB Verkehrsverbund Berlin-Brandenburg GmbH
- Baustellen, Sperrungen und sonstige Störungen — dl-de-by-2.0 — Digitale Plattform Stadtverkehr Berlin
- Nextbike GBFS (nextbike Berlin) — CC0 1.0 — atribución no exigida

Las menciones geolocalizadas son sintéticas y generadas por el repositorio. No tienen origen externo.