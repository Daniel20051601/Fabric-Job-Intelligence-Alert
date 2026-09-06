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

# # 🚨Agrega la tabla con los empleos a enviar por Email y genera el contenido de este

# MARKDOWN ********************

# ## Importación de librerías

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Inicialización de la Tabla de Control
# Creación de la tabla Delta `Gold.job_alerts` si aún no existe. Esta tabla lleva la trazabilidad de qué vacantes ya han sido notificadas (`is_sent`) y en qué momento (`sent_at`).

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS Gold.job_alerts(
# MAGIC     hash_id STRING,
# MAGIC     is_sent BOOLEAN,
# MAGIC     sent_at TIMESTAMP
# MAGIC )

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_jobs = spark.table('Gold.jobs')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Registro de nuevos empleos
# Lectura de las vacantes procesadas en `Gold.jobs` e inserción incremental en `Gold.job_alerts`.

# CELL ********************

target_table = DeltaTable.forName(spark, 'Gold.job_alerts')

target_table.alias('target').merge(
    df_jobs.alias('source'),
    'target.hash_id == source.hash_id'
).whenNotMatchedInsert( values = {
    'hash_id' : 'source.hash_id',
    'is_sent' : F.lit(False),
    'sent_at' : F.lit(None)
    }
).execute()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM Gold.job_alerts

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Selección y Filtrado de Ofertas para la Alerta
# Cruce entre los detalles del puesto y la tabla de alertas para obtener únicamente vacantes pendientes de envío

# MARKDOWN ********************

# ## 

# CELL ********************

df_jobs = (
    spark.table('Gold.jobs')
    .join(spark.table('Gold.job_alerts'), on = 'hash_id', how = 'inner')
    .orderBy(F.col('match_score').desc())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_jobs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_jobs = df_jobs.filter(F.col('is_sent') == False).limit(6)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_jobs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Construcción del Cuerpo del Correo
# Si `jobs` está vacío el notebook da como salida `NO_JOBS` y termine la ejecució, en caso contrario se enviará el Email

# CELL ********************

jobs = df_jobs.collect()

if not jobs:
    notebookutils.notebook.exit("NO_JOBS")

html = """
<h2>🎯 Nuevas ofertas de trabajo</h2>
<p>Estas son las ofertas más relevantes encontradas:</p>
"""

for job in jobs:
    location = f"Remote: {', '.join(job['location'])}" if job["remote"] else ", ".join(job["location"])
    html += f"""
    <hr>
    <h3>{job['title']}</h3>
    <p><b>🏢 Empresa:</b> {job['company']}</p>
    <p><b>📍 Location:</b> {location}</p>
    <p><b>🎯 Match Score:</b> {job['match_score']}%</p>
    <p><b>🛠 Skills:</b> {', '.join(job['matched_skills'])}</p>
    <p><a href="{job['url']}">Ver oferta →</a></p>
    """

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Actualización de Estado de Envíos
# Actualización en `Gold.job_alerts` marcando como notificadas (`is_sent = True`, `sent_at = current_timestamp()`) las 6 vacantes procesadas en este ciclo.

# CELL ********************

target_alert_table = DeltaTable.forName(spark, 'Gold.job_alerts')

target_alert_table.alias('target').merge(
    df_jobs.alias('source'),
    'target.hash_id = source.hash_id'
).whenMatchedUpdate(set = {
    'is_sent' : F.lit(True),
    'sent_at' : F.current_timestamp()
}
).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM Gold.job_alerts

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Retorno del Pipeline

# CELL ********************

notebookutils.notebook.exit(html)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
