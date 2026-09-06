# 💼 Fabric Job Intelligence Alert

Plataforma de inteligencia de empleo construida en **Microsoft Fabric** para ingerir ofertas desde múltiples fuentes, normalizarlas en un Lakehouse medallion, calcular relevancia para perfiles Data/AI/Analytics y generar alertas automáticas por correo.

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-Lakehouse%20%26%20Pipelines-7A1FA2?style=flat-square)](https://learn.microsoft.com/fabric/)
[![Python](https://img.shields.io/badge/Implementation-Python-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Tables-0099B0?style=flat-square)](https://learn.microsoft.com/fabric/data-engineering/lakehouse-overview)
[![Orchestration](https://img.shields.io/badge/Orchestration-Fabric%20Pipeline-F5A623?style=flat-square)](https://learn.microsoft.com/fabric/data-factory/data-pipelines)

---

## 🎯 El Problema

Buscar ofertas de empleo en distintas fuentes es caótico:
- Formatos heterogéneos
- Duplicados abundantes
- Descripciones incompletas
- Campos inconsistentes
- Mucho ruido para identificar oportunidades reales

**La solución:** Una capa de inteligencia que captura, estandariza, prioriza y notifica automáticamente.

---

## 🏗️ Visión de la Solución

```mermaid
flowchart LR
    A[Fuentes de empleo] --> B[Bronze / Files]
    B --> C[Silver / Normalización]
    C --> D[Silver / Matching]
    D --> E[Gold / Curated]
    D --> F[Gold / Alerts]
    E --> G[Notebook Alertas]
    F --> G
    G --> H[📧 Email]
    E --> I[Análisis]
```

**Arquitectura Medallion:**
- 🔴 **Bronze**: Datos crudos de cada fuente
- 🟢 **Silver**: Estructuras unificadas, limpieza y relevancia
- 🟡 **Gold**: Tablas listas para consumo operativo y analítico
- ⚙️ **Pipeline**: Orquestación end-to-end y disparo de alertas

---

## 📂 Estructura del Repositorio

```
Fabric-Job-Intelligence-Alert/
├── Sources Notebook/
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
└── PL_Master.DataPipeline/
    ├── pipeline-content.json
    └── .schedules
```

| Componente | Responsabilidad |
|---|---|
| **Sources** | Ingesta desde APIs externas → archivos crudos en Bronze |
| **Silver** | Normalización, limpieza, deduplicación, scoring |
| **Gold** | Materialización de tablas finales y generación de alertas |
| **Lakehouse** | Almacenamiento medallion y configuración |
| **Pipeline** | Orquestación end-to-end y lógica de envío |

---

## 🔄 Flujo de Datos

```mermaid
flowchart TD
    R[Remotive API] --> BR["📁 Files/Bronze/Remotive/*.json"]
    GOB[Get On Board API] --> BG["📁 Files/Bronze/GetOnBoard/*.json"]

    BR --> ST["🔧 NB_transform_jobs"]
    BG --> ST

    ST --> SJ["🗄️ Silver.job_matches"]

    SJ --> MJ["🎯 NB_match_jobs"]
    MJ --> GA["🗄️ Gold.job_alerts"]

    MJ --> GJ["📋 NB_gold_jobs"]
    GJ --> GJT["🗄️ Gold.jobs"]

    GJT --> AJ["📧 NB_alert_jobs"]
    GA --> AJ
    AJ --> EM["Office 365 Email"]
```

### Lectura del Flujo

**1️⃣ Ingesta**
- Consumo desde Remotive y Get on Board
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

---

## 🛠️ Tecnologías

| Tecnología | Rol |
|---|---|
| Microsoft Fabric Lakehouse | Almacenamiento unificado |
| Fabric Notebooks / PySpark | Ingesta y transformación |
| Delta Tables | Persistencia incremental |
| Fabric Data Pipeline | Orquestación |
| Office 365 Connector | Envío de alertas |
| Python `requests` | Consumo de APIs |
| SQL | Esquemas y validación |

---

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

---

## 📋 Implementación por Capa

### 🔴 Bronze (Sources)
**Notebooks:** `NB_ingest_remotive` | `NB_ingest_getonboard`

Consumen APIs externas, normalizan mínimamente y guardan JSON en:
- `Files/Bronze/Remotive/`
- `Files/Bronze/GetOnBoard/`

### 🟢 Silver (Transformación)
**Notebooks:** `NB_transform_jobs` | `NB_match_jobs`

Unifican esquemas, limpian contenido, calculan relevancia y escriben:
- `Silver.job_matches`

### 🟡 Gold (Publicación)
**Notebooks:** `NB_gold_jobs` | `NB_alert_jobs`

Materializan tablas listas para consumo:
- `Gold.jobs`
- `Gold.job_alerts`

---

## ⚙️ Orquestación (PL_Master)

El pipeline ejecuta en este orden (diariamente a las 08:00):

1. Ingest From Get On Board
2. Ingest from Remotive
3. Transform Jobs
4. Match Jobs
5. Load Gold Jobs
6. Job Alerts

**Lógica final:**
- ✅ Si hay nuevas vacantes → envía correo
- ⏭️ Si no hay → omite envío

---

## 🚀 Preparación para Replicarlo

### Requisitos Previos
- ✓ Workspace de Microsoft Fabric con permisos para Lakehouse, Notebooks, Pipeline
- ✓ Acceso a Internet (APIs externas)
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
  ├── Silver.job_matches
  ├── Gold.jobs
  └── Gold.job_alerts
```

---

## ✅ Validación

Una ejecución correcta muestra:

- 📁 Nuevos archivos JSON en Bronze
- 📊 Filas en `Silver.job_matches`
- 📈 Registros en `Gold.jobs`
- 🔔 Control en `Gold.job_alerts`
- 📧 Correo HTML con vacantes priorizadas (si hay nuevas)

---

## 📊 Estado del Proyecto

Base funcional completa: ingesta → transformación → scoring → persistencia → alertas.
Diseñado para ampliarse con nuevas fuentes sin romper el modelo medallion.

---

## 🎁 Resultado Final

Una plataforma que:
- 🔍 **Captura** datos de múltiples fuentes
- ⚙️ **Estandariza** campos y estructuras
- 📌 **Prioriza** ofertas por relevancia
- 📋 **Publica** tablas analíticas
- 📧 **Notifica** automáticamente las mejores oportunidades
