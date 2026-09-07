# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c51f0078-90ee-4fb3-a75e-1f5ab5bb6dbe",
# META       "default_lakehouse_name": "LH_Medallion",
# META       "default_lakehouse_workspace_id": "c6b1c951-0a9a-4abc-b7e8-8728316be7e5",
# META       "known_lakehouses": [
# META         {
# META           "id": "c51f0078-90ee-4fb3-a75e-1f5ab5bb6dbe"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 🏅 Ingesta y Procesamiento de Empleos

# MARKDOWN ********************

# ### Configuración y Dependencias

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Inicialización del Esquema Gold

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS Gold

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Definición de la Tabla Target `Gold.jobs`

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS Gold.jobs(
# MAGIC     hash_id STRING,
# MAGIC     title STRING,
# MAGIC     company STRING,
# MAGIC     description STRING,
# MAGIC     location ARRAY<STRING>,
# MAGIC     remote BOOLEAN,
# MAGIC     salary STRING,
# MAGIC     skills ARRAY<STRING>,
# MAGIC     source STRING,
# MAGIC     url STRING,
# MAGIC     published_at TIMESTAMP,
# MAGIC     match_score INTEGER,
# MAGIC     matched_skills ARRAY<STRING>,
# MAGIC     ingestion_date TIMESTAMP,
# MAGIC     last_update TIMESTAMP
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Extracción de Datos Validados 

# CELL ********************

df_jobs = spark.table('silver.job_matches')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Merge Incremental en Gold.jobs

# CELL ********************

target_table = DeltaTable.forName(spark, 'Gold.jobs')

target_table.alias('target').merge(
    df_jobs.alias('source'),
    'target.hash_id = source.hash_id'
).whenMatchedUpdate(set = {
    "title": "source.title",
    "company": "source.company",
    "description": "source.description",
    "location": "source.location",
    "remote": "source.remote",
    "salary": "source.salary",
    "skills": "source.skills",
    "source": "source.source",
    "url": "source.url",
    "published_at": "source.published_at",
    "match_score": "source.match_score",
    "matched_skills": "source.matched_skills",
    "last_update": "current_timestamp()"  
}
).whenNotMatchedInsert(values = {
    "hash_id": "source.hash_id",
    "title": "source.title",
    "company": "source.company",
    "description": "source.description",
    "location": "source.location",
    "remote": "source.remote",
    "salary": "source.salary",
    "skills": "source.skills",
    "source": "source.source",
    "url": "source.url",
    "published_at": "source.published_at",
    "match_score": "source.match_score",
    "matched_skills": "source.matched_skills",
    "ingestion_date": "current_timestamp()",
    "last_update": "current_timestamp()"
}

).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Validación de Carga y Control de Calidad

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM Gold.jobs

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
