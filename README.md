# JG Analítica – Desempleo en Chile · Data Pipeline (Airflow + Docker + PostgreSQL + Power BI)

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)](#)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-Neon%20Cloud-336791?logo=postgresql&logoColor=white)](#)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-Orquestación-017CEE?logo=apacheairflow&logoColor=white)](#)
[![Docker](https://img.shields.io/badge/Docker-Containerizado-2496ED?logo=docker&logoColor=white)](#)
[![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?logo=powerbi&logoColor=black)](#)
[![GitHub Actions](https://img.shields.io/badge/CI-GitHub%20Actions-2088FF?logo=githubactions&logoColor=white)](#)
[![Pipeline](https://img.shields.io/badge/Pipeline-Automatizado-4CAF50)](#)
[![Última actualización](https://img.shields.io/badge/Última%20actualización-diaria-blue)](#)

> Análisis del **desempleo en Chile** utilizando datos oficiales del **Banco Central** con un pipeline moderno de datos.  
> Ingesta **Python → PostgreSQL (Neon Cloud)**, orquestación con **Apache Airflow en Docker**  
> y visualización analítica en **Power BI** con actualización automática vía **Power BI REST API**.

**Demo:** Dashboards analíticos sobre mercado laboral chileno (2015–2026)  
**Diseño:** Dark analytics con enfoque de storytelling económico.

---

# Índice
1. [Elevator pitch](#1-elevator-pitch)  
2. [Arquitectura](#2-arquitectura)  
3. [Dashboards (qué preguntas responden)](#3-dashboards-qué-preguntas-responden)  
4. [KPIs clave](#4-kpis-clave)  
5. [Stack usado](#5-stack-usado-en-el-proyecto)  
6. [Impacto analítico](#6-impacto-analítico)  
7. [Cómo correr el proyecto](#7-cómo-correr-el-proyecto)  
8. [Automatización diaria](#8-automatización-diaria)  
9. [CI con GitHub Actions](#9-ci-con-github-actions)  
10. [Estructura del repo](#10-estructura-del-repo)

---

# 1) Elevator pitch

Este proyecto implementa un **pipeline de datos end-to-end** para analizar la evolución del desempleo en Chile utilizando datos del **Banco Central de Chile**.

El sistema:

- Consume datos desde la **API del Banco Central**
- Procesa información mediante **arquitectura Medallion**
- Almacena datos en **PostgreSQL cloud (Neon)**
- Orquesta ejecuciones con **Apache Airflow**
- Actualiza dashboards automáticamente mediante **Power BI REST API**

> **Resultado:** un sistema analítico automatizado que permite monitorear el mercado laboral chileno con actualización diaria.

---

# 2) Arquitectura


API Banco Central → Python ETL (pandas) → PostgreSQL (Neon Cloud) → (Bronze → Silver → Gold) → Power BI Dataset → Dashboards analíticos


- Orquestación: Apache Airflow (Docker)

- Flujo del DAG: Bronze ingestion → Silver transformation → Gold analytics layer → Power BI dataset refresh

> El pipeline se ejecuta automáticamente **todos los días a las 06:00 AM**.

---

# 3) Dashboards (qué preguntas responden)

## 3.1 Panorama general del mercado laboral

**Preguntas**

- ¿Cómo ha evolucionado el desempleo en Chile desde 2015?
- ¿El nivel actual es mayor que en crisis anteriores?
- ¿El aumento reciente responde a factores estructurales o cíclicos?

**Aporta al análisis**

- Contexto histórico del mercado laboral  
- Comparación pandemia vs post-pandemia  

<img width="2244" height="1261" alt="{300A64DC-4E0F-4AFE-8931-0EF2A382BBA7}" src="https://github.com/user-attachments/assets/5cac7f39-7b7b-4a9a-ac3e-2d7c18cdab41" />


---

## 3.2 Brecha de género en el desempleo

**Preguntas**

- ¿Existe una brecha estructural entre desempleo masculino y femenino?
- ¿Cómo evoluciona esta diferencia en el tiempo?
- ¿Las variaciones responden a ciclos económicos?

**Aporta al análisis**

- Identificación de desigualdades persistentes  
- Evaluación de convergencia entre tasas laborales  

<img width="2254" height="1253" alt="{D516FE2C-88F6-4D90-87A7-089AAB10EBE6}" src="https://github.com/user-attachments/assets/d95dc1c0-1cfb-4a8b-b778-0cc9504a766b" />


---

## 3.3 Contexto macroeconómico

**Preguntas**

- ¿Cómo se relaciona el desempleo con la actividad económica (IMACEC)?
- ¿Existe correlación entre inflación y desempleo?
- ¿Cómo influyen variables macroeconómicas en el mercado laboral?

**Aporta al análisis**

- Interpretación macroeconómica del desempleo  
- Identificación de relaciones económicas relevantes  

<img width="2245" height="1255" alt="{8C30BF1E-2A04-4B7F-8633-4C70CB2E5CB7}" src="https://github.com/user-attachments/assets/721c681e-4b57-475e-a223-be661a4a707a" />


---

## 3.4 Desempleo regional

**Preguntas**

- ¿Qué regiones presentan mayor desempleo?
- ¿Dónde se concentra el desempleo estructural?
- ¿Existen diferencias territoriales significativas?

**Aporta al análisis**

- Identificación de heterogeneidad territorial  
- Apoyo a análisis regional del mercado laboral  

<img width="2250" height="1260" alt="{7BB5E94A-0A78-439C-8755-59143A705B86}" src="https://github.com/user-attachments/assets/0a8e0e24-c592-4a5d-9a45-2cc10b92ce75" />


---

# 4) KPIs clave

- **Tasa de desempleo nacional (%)**
- **Desempleo por género**
- **Brecha de desempleo**
- **IMACEC (actividad económica)**
- **Inflación (IPC)**
- **UF**
- **Desempleo promedio por región**
- **Región con mayor desempleo**
- **Región con menor desempleo**

> Los datos se actualizan automáticamente mediante el pipeline diario.

---

# 5) Stack usado en el proyecto

- **Python** (pandas)
- **PostgreSQL** (Neon Cloud)
- **Apache Airflow**
- **Docker / Docker Compose**
- **Power BI**
- **Power BI REST API**
- **GitHub Actions (CI)**
- **Medallion Architecture (Bronze / Silver / Gold)**

---

# 6) Impacto analítico

Este proyecto demuestra cómo construir un **pipeline moderno de datos económicos**.

Beneficios:

- Automatización completa del flujo de datos
- Actualización diaria sin intervención manual
- Arquitectura escalable mediante capas Medallion
- Integración directa con dashboards analíticos

Aplicaciones:

- Monitoreo del mercado laboral  
- Análisis macroeconómico  
- Investigación económica  
- Dashboards automatizados

---

# 7) Cómo correr el proyecto

### 1) Clonar el repo


- git clone https://github.com/<tu-usuario>/<tu-repo>.git
- cd <tu-repo>


### 2) Crear entorno virtual


- python -m venv .venv
- ..venv\Scripts\activate


### 3) Instalar dependencias


- pip install -r requirements.txt


### 4) Crear archivo `.env`


- copy .env.example .env


Configurar variables:


- DATABASE_URL=
- POWERBI_CLIENT_ID=
- POWERBI_CLIENT_SECRET=
- POWERBI_TENANT_ID=
- POWERBI_DATASET_ID=


### 5) Levantar Airflow con Docker


- docker compose up -d


### 6) Ejecutar DAG manual


- airflow dags trigger bc_medallion_pipeline


---

# 8) Automatización diaria

El pipeline se ejecuta automáticamente mediante **Apache Airflow**.

- Horario: 06:00 AM diario
- Flujo: Bronze ingestion → Silver transformation → Gold analytics layer → Power BI dataset refresh

> Una vez finalizada la capa **Gold**, el pipeline ejecuta un **refresh automático del dataset en Power BI**.

<img width="1884" height="819" alt="Image" src="https://github.com/user-attachments/assets/05f022fb-27a7-42ee-8e76-0ea13c90a944" />

---

# 9) CI con GitHub Actions

Cada vez que se actualiza el repositorio:

El pipeline de **CI**:

- instala dependencias
- valida el entorno Python
- verifica conexiones a la base de datos
- ejecuta validaciones del proyecto

> Esto asegura que el pipeline sea **reproducible y estable antes del despliegue**.

<img width="1871" height="816" alt="Image" src="https://github.com/user-attachments/assets/ead134fa-a35b-447c-9f18-63651c28b5f1" />

---

⭐ **Proyecto orientado a Data Engineering moderno**

Este repositorio demuestra habilidades en:

- Data Pipelines
- Arquitectura Medallion
- Orquestación con Airflow
- Infraestructura con Docker
- Integración con APIs
- Automatización de dashboards
