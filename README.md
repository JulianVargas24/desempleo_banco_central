# Análisis del desempleo en Chile · Data Pipeline + Power BI

> Python · PostgreSQL (Neon) · Apache Airflow · Docker · Power BI · GitHub Actions · Automatizado · Última actualización diaria
> Pipeline end-to-end para ingestión, procesamiento y visualización de datos económicos del Banco Central de Chile, utilizando arquitectura Medallion (Bronze / Silver / Gold) y actualización automática de dashboards en Power BI.
> Demo: dashboards interactivos con análisis estructural del mercado laboral chileno (2015-2026).

---

## Índice

1. [Elevator pitch]
2. [Arquitectura]
3. [Dashboards (qué preguntas responden)]
4. [KPIs clave]
5. [Stack usado]
6. [Impacto analítico]
7. [Cómo correr el proyecto]
8. [Automatización diaria]
9. [Estructura del repo]

---

## 1) Elevator pitch

Este proyecto construye un pipeline de datos completamente automatizado para analizar la evolución del desempleo en Chile utilizando datos oficiales del Banco Central.

El pipeline:

- Consume datos desde la API del Banco Central de Chile
- Procesa la información mediante arquitectura Medallion (Bronze → Silver → Gold)
- Almacena los datos en PostgreSQL cloud (Neon)
- Orquesta la ejecución diaria mediante Apache Airflow en Docker
- Actualiza automáticamente los dashboards mediante Power BI REST API

> **Resultado:** Un sistema analítico reproducible que permite monitorear dinámica del desempleo, brecha de género, contexto macroeconómico y diferencias regionales con actualización diaria.

---

## 2) Arquitectura

API Banco Central
↓
Python (pandas – ingestión)
↓
Bronze Layer (PostgreSQL / Neon)
↓
Silver Layer (limpieza y normalización)
↓
Gold Layer (modelo analítico incremental)
↓
Power BI (modelo semántico + dashboards)

- Orquestación: Apache Airflow (Docker)

- Automatización: GitHub Actions (CI) y Power BI REST API (refresh automático)

Orquestación diaria mediante Airflow DAG ejecutado a las 06:00 AM.

3) Dashboards (qué preguntas responden)
3.1 Panorama general del mercado laboral

Preguntas

• ¿Cómo ha evolucionado el desempleo en Chile desde 2015?
• ¿Cuál es el nivel actual del desempleo nacional?
• ¿Cómo se compara con periodos de crisis anteriores?

Aporta al análisis

• Identificación de tendencias estructurales del mercado laboral
• Comparación entre pandemia, post-pandemia y periodos previos

Sugerencia

![Panorama General](./images/panorama_general.png)
3.2 Brecha de género en el desempleo

Preguntas

• ¿Existe una brecha estructural entre desempleo masculino y femenino?
• ¿Cómo evoluciona esta diferencia en el tiempo?
• ¿La brecha responde a ciclos económicos o es estructural?

Aporta al análisis

• Identificación de desigualdades persistentes en el mercado laboral
• Evaluación de convergencia entre tasas de desempleo

Sugerencia

![Brecha de género](./images/brecha_genero.png)
3.3 Contexto macroeconómico

Preguntas

• ¿Cómo se relaciona el desempleo con la actividad económica (IMACEC)?
• ¿Existe relación entre inflación y desempleo?
• ¿Cómo influyen variables macroeconómicas en el mercado laboral?

Aporta al análisis

• Interpretación macroeconómica del desempleo
• Identificación de correlaciones entre actividad económica y empleo

Sugerencia

![Contexto macroeconómico](./images/contexto_macro.png)
3.4 Desempleo regional

Preguntas

• ¿Qué regiones presentan mayor desempleo?
• ¿Dónde se concentra el desempleo estructural?
• ¿Existen diferencias territoriales significativas?

Aporta al análisis

• Identificación de heterogeneidad regional
• Apoyo a análisis territorial del mercado laboral

Sugerencia

![Desempleo regional](./images/desempleo_regional.png)
4) KPIs clave

Tasa de desempleo nacional (%)
Desempleo por género (hombres / mujeres)
Brecha de desempleo de género
IMACEC (actividad económica)
Inflación (IPC)
UF (indicador financiero)
Desempleo promedio por región
Región con mayor desempleo
Región con menor desempleo

Los datos se actualizan diariamente desde el pipeline.

5) Stack usado en el proyecto

Lenguajes

Python (pandas)

Orquestación

Apache Airflow

Infraestructura

Docker
Docker Compose

Bases de datos

PostgreSQL (Neon Cloud)

Visualización

Power BI

Automatización

Power BI REST API (refresh dataset)

CI/CD

GitHub Actions

Arquitectura

Medallion Architecture (Bronze / Silver / Gold)

6) Impacto analítico

Este proyecto demuestra cómo construir un pipeline de datos reproducible para análisis económico.

Beneficios del enfoque:

Automatización completa del flujo de datos
Actualización diaria sin intervención manual
Separación de capas de datos mediante arquitectura Medallion
Pipeline reproducible y escalable

Aplicaciones potenciales

Análisis económico
Monitoreo de mercado laboral
Investigación macroeconómica
Dashboards analíticos para toma de decisiones

7) Cómo correr el proyecto

Clonar repositorio

git clone https://github.com/TU_USUARIO/banco-central-data-pipeline.git
cd banco-central-data-pipeline

Crear entorno virtual

python -m venv .venv
.venv\Scripts\activate

Instalar dependencias

pip install -r requirements.txt

Crear archivo .env

copy .env.example .env

Configurar variables

DATABASE_URL
POWERBI_CLIENT_ID
POWERBI_CLIENT_SECRET
POWERBI_TENANT_ID
POWERBI_DATASET_ID

Levantar Airflow con Docker

docker compose up -d

Ejecutar pipeline manual

airflow dags trigger bc_medallion_pipeline
8) Automatización diaria

El pipeline se ejecuta automáticamente mediante Apache Airflow.

Horario

06:00 AM diario

Flujo del DAG

Bronze ingestion
      ↓
Silver transformation
      ↓
Gold analytics layer
      ↓
Power BI dataset refresh

Después de finalizar la capa Gold, Airflow llama a la Power BI REST API para actualizar el dataset automáticamente.

9) Integración CI (GitHub Actions)

Cada vez que se actualiza el código en GitHub:

El pipeline de CI:

• instala dependencias
• valida el entorno
• verifica conexiones a la base de datos
• ejecuta validaciones básicas del proyecto

Esto asegura que el pipeline pueda ejecutarse correctamente antes de desplegar cambios.

10) Estructura del repo
project
│
├─ dags/
│   bc_medallion_pipeline.py
│
├─ src/
│   ingesta/
│   transformacion/
│   datos_gold/
│
├─ utils/
│   conexion_postgre.py
│   funciones.py
│
├─ dashboards/
│   desempleo_chile.pbix
│
├─ images/
│   panorama_general.png
│   brecha_genero.png
│   contexto_macro.png
│   desempleo_regional.png
│
├─ requirements.txt
├─ docker-compose.yml
└─ README.md
