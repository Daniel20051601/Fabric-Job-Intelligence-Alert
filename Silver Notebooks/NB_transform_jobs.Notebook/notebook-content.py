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

# # ♻️Transformación y normalización de las ofertas de trabajo

# CELL ********************

from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, StructField, StringType, DateType
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Leemos las ofertas de la capa bronze

# MARKDOWN ********************

# #### Remotive

# CELL ********************

df_remotive = (
    spark.read.json(
    "Files/Bronze/Remotive/*",
    multiLine=True
    )
    .select(
        F.trim(F.col('title')).alias('title'),
        F.trim(F.col('company_name')).alias("company"),
        F.col("description"),
        F.transform(
            F.split(F.col("candidate_required_location"), ","),
            lambda x: F.initcap(F.trim(x))
        ).alias('location'),
        F.lit(True).cast('boolean').alias("remote"),
        F.col('salary'),
        F.col('tags').alias('skills'),
        F.lit('Remotive').alias('source'),
        F.col('url'),
        F.to_timestamp('publication_date').alias('published_at')
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_remotive)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Get On Board

# CELL ********************

df_getonboard = (
    spark.read.json(
    "Files/Bronze/GetOnBoard/*",
    multiLine=True  
    ).select(
        F.trim(F.col("attributes.title")).alias("title"),
        F.trim(F.col("attributes.company.data.attributes.name")).alias("company"),
        F.col("attributes.description").alias("description"),
        F.col("attributes.countries").alias("location"),
        F.col("attributes.remote").cast("boolean").alias("remote"),
        F.concat_ws(" - ",F.col('attributes.min_salary'), F.col("attributes.max_salary")).cast('string').alias("salary"),
        F.col("attributes.tags.data.attributes.name").alias('skills'),
        F.lit('Get On Board').alias('source'),
        F.col("links.public_url").alias("url"),
        F.to_timestamp(F.col("attributes.published_at").cast("long")).alias("published_at"), 
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_getonboard)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Unimos las diferentes fuentes en un mismo Dataframe

# CELL ********************

df_jobs = df_remotive.unionByName(df_getonboard)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Transformamos los datos

# MARKDOWN ********************

# ### ♻️Eliminamos duplicados

# CELL ********************

df_jobs = df_jobs.drop_duplicates()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ♻️Rellenamos campos nulos

# CELL ********************

df_jobs = df_jobs.fillna('Unknown')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Rellenamos el campo salario para todas las filas

# CELL ********************

df_jobs = df_jobs.withColumn(
    'Salary',
    F.when(
        F.trim(F.col('salary')) == "",
        'Unknown'
    ).otherwise(F.col('salary'))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ♻️Normalizamos

# MARKDOWN ********************

# Normalizamos `skills`

# CELL ********************

df_jobs = df_jobs.withColumn(
    'skills',
    F.transform(
        'skills',
        lambda x: F.lower(F.trim(x))
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### ♻️Limpiamos el HTML de la descripción

# CELL ********************

df_jobs = df_jobs.withColumn(
    "description",
    F.regexp_replace(
        F.col("description"),
        "<[^>]*>",
        " "
    )
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

# MARKDOWN ********************

# ## Verificamos el schema final de los datos

# CELL ********************

df_jobs.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Guardamos el Dataframe como tabla en la capa Silver

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS  Silver

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_jobs.write.saveAsTable(
    "Silver.jobs_postings",
    mode="overwrite",
    format='delta'
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 
