# 💼 Fabric Job Intelligence Alert

Plataforma de inteligencia de empleo construida en **Microsoft Fabric** para ingerir ofertas desde múltiples fuentes, normalizarlas en un Lakehouse medallion, calcular relevancia para perfiles Data/AI/Analytics y generar alertas automáticas por correo.

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-Lakehouse%20%26%20Pipelines-7A1FA2?style=flat-square)](https://learn.microsoft.com/fabric/)
[![Power BI](https://img.shields.io/badge/Analytics-Power%20BI-F2C811?style=flat-square&logo=powerbi&logoColor=white)](https://powerbi.microsoft.com/)
[![PySpark](https://img.shields.io/badge/Processing-PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org/docs/latest/api/python/)
[![SQL](https://img.shields.io/badge/Query-SQL-4479A1?style=flat-square)](https://www.postgresql.org/docs/)
[![Medallion](https://img.shields.io/badge/Architecture-Medallion%20Model-2E86C1?style=flat-square)](https://learn.microsoft.com/fabric/data-engineering/lakehouse-overview)
[![Python](https://img.shields.io/badge/Implementation-Python-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Tables-0099B0?style=flat-square)](https://learn.microsoft.com/fabric/data-engineering/lakehouse-overview)
[![Orchestration](https://img.shields.io/badge/Orchestration-Fabric%20Pipeline-F5A623?style=flat-square)](https://learn.microsoft.com/fabric/data-factory/data-pipelines)


## 🎯 El Problema

Buscar ofertas de empleo en distintas fuentes es caótico:
- Formatos heterogéneos
- Duplicados abundantes
- Descripciones incompletas
- Campos inconsistentes
- Mucho ruido para identificar oportunidades reales

**La solución:** Una capa de inteligencia que captura, estandariza, prioriza y notifica automáticamente.


## 🏗️ Visión de la Solución

```mermaid
flowchart LR
    A[Job sources] --> B[Bronze / Files]
    B --> C[Silver / Cleaning and Matching]
    C --> E[Gold / Jobs and alerts]
    E --> H[Email]
    E --> I[Semantic Model]
```

**Arquitectura Medallion:**
- 🔴 **Bronze**: Datos crudos de cada fuente
- 🟢 **Silver**: Estructuras unificadas, limpieza y relevancia
- 🟡 **Gold**: Tablas listas para consumo operativo y analítico

## 📂 Estructura del Repositorio

```text
Fabric-Job-Intelligence-Alert/
├── Sources Notebooks/
│   ├── NB_ingest_remotive.Notebook/
│   └── NB_ingest_getonboard.Notebook/
├── Silver Notebooks/
│   ├── NB_transform_jobs.Notebook/
│   └── NB_match_jobs.Notebook/
├── Gold Notebooks/
│   ├── NB_gold_jobs.Notebook/
│   └── NB_alert_jobs.Notebook/
├── LH_Medallion.Lakehouse/
│   ├── alm.settings.json
│   ├── lakehouse.metadata.json
│   └── shortcuts.metadata.json
├── PL_Master.DataPipeline/
│   ├── pipeline-content.json
│   └── .schedules
├── Jobs Semantic Model.SemanticModel/
└── README.md
```

| Componente | Responsabilidad |
|---|---|
| **Sources Notebooks** | Ingesta desde APIs externas hacia archivos en Bronze |
| **Silver Notebooks** | Normalización, limpieza, deduplicación, scoring |
| **Gold Notebooks** | Materialización de tablas finales y generación de alertas |
| **Lakehouse** | Almacenamiento medallion y configuración |
| **DataPipeline** | Orquestación end-to-end y lógica de envío |

## 🔄 Flujo de Datos

```mermaid
flowchart TD
    R[Remotive API] --> BR["📁 Files/Bronze/Remotive/*.json"]
    GOB[Get On Board API] --> BG["📁 Files/Bronze/GetOnBoard/*.json"]

    BR --> ST["🔧 NB_transform_jobs"]
    BG --> ST
    ST --> JP["Silver.jobs_postings"]

    JP --> MJ["🎯 NB_match_jobs"]
    MJ --> SJ["🗄️ Silver.job_matches"]

    SJ --> GJ["📋 NB_gold_jobs"]
    GJ --> GJT["🗄️ Gold.jobs"]

    GJT --> NAJ[NB_alert_jobs]
    NAJ --> GA["🗄️ Gold.job_alerts"]

    GJT --> SM[📊 Semantic Model]
    GA  --> SM

    GJT --> AJ["📧 NB_alert_jobs"]
    GA --> AJ
    AJ --> EM["Office 365 Email"]
```
### Lectura del Flujo

**1️⃣ Ingesta**
- Consumo desde las fuentes Remotive y Get on Board en principio
- Persistencia como JSON para trazabilidad

**2️⃣ Transformación**
- Homogenización de columnas y estructuras
- Consolidación entre fuentes
- Eliminación de duplicados
- Limpieza de HTML

**3️⃣ Matching**
- Evaluación de: títulos, skills, descripción, ubicación
- Generación de `match_score`
- Filtro automático por relevancia
- Hash estable para identificar ofertas

**4️⃣ Gold**
- `Gold.jobs`: catálogo curado
- `Gold.job_alerts`: control de notificaciones

**5️⃣ Alertas**
- Selección de oportunidades pendientes
- Construcción de correo HTML
- Envío solo si hay nuevas vacantes

**6️⃣ Semantic Model**
- Capa semántica construida directamente sobre las tablas Gold
- Definición de relaciones
- Tablas listas para análisis y visualización en Power BI

## 🛠️ Tecnologías

| Tecnología | Rol |
|---|---|
| Microsoft Fabric Lakehouse | Almacenamiento unificado |
| Fabric Notebooks / PySpark | Ingesta y transformación |
| Delta Tables | Persistencia incremental |
| Fabric Data Pipeline | Orquestación |
| Fabric Semantic Model | Exposición de datos curados |
| Office 365 Connector | Envío de alertas |
| Python `requests` | Consumo de APIs |
| SQL | Esquemas y validación |

## 💡 Lógica de Negocio

### Criterio de Relevancia
El sistema asigna un `match_score` combinando:
- Coincidencia de título con perfiles objetivo
- Presencia de skills esperadas
- Correspondencia de ubicaciones
- Reglas de normalización aplicadas al texto

### Deduplificación
Consolidación y eliminación de duplicados antes de persistir. En Gold se usa `hash_id` para identificar ofertas de forma consistente.

### Control de Alertas
`Gold.job_alerts` evita reenvíos. El notebook de alertas procesa solo registros no enviados y marca el estado de envío.

### Limpieza de Contenido
Las descripciones se limpian de HTML y los campos se homogenizan para reducir fricción downstream.


## 📋 Implementación por Capa

### 🔴 Bronze (Sources)
**Notebooks:** `NB_ingest_remotive` | `NB_ingest_getonboard`

Consumen APIs externas, normalizan mínimamente y guardan JSON en:
- `Files/Bronze/Remotive/`
- `Files/Bronze/GetOnBoard/`

### 🟢 Silver (Transformación)
**Notebooks:** `NB_transform_jobs` | `NB_match_jobs`

Unifican esquemas, limpian contenido, calculan relevancia y escriben:
- `Silver.jobs_postings`
- `Silver.job_matches`

### 🟡 Gold (Publicación)
**Notebooks:** `NB_gold_jobs` | `NB_alert_jobs`

Materializan tablas listas para consumo:
- `Gold.jobs`
- `Gold.job_alerts`

## ⚙️ Orquestación (PL_Master)

El pipeline ejecuta en este orden (diariamente a las 08:00am):

1. Ingest From Get On Board
2. Ingest from Remotive
3. Transform Jobs
4. Match Jobs
5. Load Gold Jobs
6. Job Alerts

**Lógica final:**
- ✅ Si el notebook de alertas detecta nuevas vacantes, se envía un correo con las ofertas priorizadas.
- ⏭️ Si no hay empleos nuevos, también se envía un correo indicando explícitamente que no se encontraron vacantes nuevas.
- 🔀 El pipeline incluye una condición que evalúa la salida del notebook de alertas para decidir qué tipo de correo enviar.

## 🚀 Preparación para Replicarlo

### Requisitos Previos
- ✓ Workspace de Microsoft Fabric con permisos para Lakehouse, Notebooks, Pipeline
- ✓ Cuenta Office 365 para envío de correos

### Recursos a Crear
```
Lakehouse:  LH_Medallion
Pipeline:   PL_Master

Notebooks (6):
  ├── NB_ingest_remotive
  ├── NB_ingest_getonboard
  ├── NB_transform_jobs
  ├── NB_match_jobs
  ├── NB_gold_jobs
  └── NB_alert_jobs

Semantic Model: Jobs Semantic Model
```
### Configuración Necesaria
- Rutas del Lakehouse en notebooks
- Referencias a esquemas/tablas
- Conexión del conector Office 365
- Programación del pipeline
- Criterios de matching (según perfiles)

### Estructura del Lakehouse
```
Files/
  └── Bronze/
      ├── Remotive/
      └── GetOnBoard/

Schemas:
  ├── Silver.jobs_postings
  ├── Silver.job_matches
  ├── Gold.jobs
  └── Gold.job_alerts
```
## ✅ Validación

Una ejecución correcta muestra:

- 📁 Nuevos archivos JSON en Bronze
- 📊 Filas en `Silver.jobs_postings` y `Silver.job_matches`
- 📈 Registros en `Gold.jobs`
- 🔔 Control en `Gold.job_alerts`
- 📧 Correo HTML con vacantes priorizadas o indicando que no hay nuevas vacantes

## 📊 Estado del Proyecto

Base funcional completa: ingesta → transformación → scoring → persistencia → alertas.
Diseñado para ampliarse con nuevas fuentes sin romper el modelo medallion.

## 🎁 Resultado Final

Una plataforma que:
- 🔍 **Captura** datos de múltiples fuentes
- ⚙️ **Estandariza** campos y estructuras
- 📌 **Prioriza** ofertas por relevancia
- 📋 **Publica** tablas analíticas
- 📧 **Notifica** automáticamente las mejores oportunidades

## 🔍 Vistas del Proyecto
### Visualización en Power BI
<img width="744" height="540" alt="image" src="https://github.com/user-attachments/assets/40b975bf-b618-4ad4-a584-f3cf3df98a89" />

### Workspace: **WS_JobPlatform** Lineage view
<img width="651" height="676" alt="image" src="https://github.com/user-attachments/assets/5844c9d7-9070-4e19-8477-65776afbef07" />

### Lakehouse: **LH_Medallion**
<img width="1002" height="491" alt="image" src="https://github.com/user-attachments/assets/4c72e025-8d2b-4a4c-a46a-04673aefd62c" />

### Pipeline: **PL_Master**
<img width="1264" height="272" alt="image" src="https://github.com/user-attachments/assets/90eae0ae-8ace-4b85-a1f6-d5c4037c4947" />

### Semantic Model: **Jobs Semantic Model**
<img width="578" height="359" alt="image" src="https://github.com/user-attachments/assets/5366f6c5-1b99-4560-835b-fa544ade9da6" />

## 🧑‍💻 Autor

**Ramón Emilio López**
- **LinkedIn:** [ramón-emilio-lopez-57a833211](https://www.linkedin.com/in/ram%C3%B3n-emilio-lopez-57a833211/)
