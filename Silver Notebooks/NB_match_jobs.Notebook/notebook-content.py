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

# # 🎯 Matching de Ofertas de Trabajo
# 
# Para determinar la relevancia de cada oferta respecto al perfil y puesto objetivo, implementamos un sistema de puntuación basado en diferentes criterios:
# 
# Título: 30 puntos
# Remote: 20 puntos
# Skills: 30 puntos
# Location: 20 puntos
# 
# Puntuación máxima: 100 puntos

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import lit
import re

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Leemos la tabla con las ofertas de empleo limpias y normalizadas

# CELL ********************

df_jobs = spark.read.table('Silver.jobs_postings')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Definimos los criterios de busqueda

# CELL ********************

target_titles = [
    "data engineer",
    "data engineering",
    "data engineer intern",
    "data engineering intern",
    "intern data engineer",
    "junior data engineer",
    "junior data engineering",
    "data engineer trainee",
    "data engineering trainee",
    "ingeniero de datos",
    "ingeniería de datos",
    "ingeniero de datos junior",
    "ingeniero de datos en prácticas",
    "practicante de ingeniería de datos",
    "data analyst",
    "data analytics",
    "data analytics intern",
    "analyst data",
    "analista de datos",
    "analítica de datos",
    "analytics engineer",
    "analytics engineering",
    "ingeniero de analítica",
    "ingeniero de analytics",
    "ai engineer",
    "ai engineering",
    "ai engineer intern",
    "ingeniero de ia",
    "ingeniero de aprendizaje automático",
    "big data engineer",
    "big data developer",
    "big data analyst",
    "ingeniero de big data",
    "ingeniero de datos big data",
    "cloud data engineer",
    "azure data engineer",
    "data platform engineer",
    "ingeniero de datos cloud",
    "ingeniero de datos en la nube"
]


target_skills = [
    "python",
    "sql",
    "pyspark",
    "spark",
    "apache spark",
    "big data",
    "azure",
    "microsoft azure",
    "microsoft fabric",
    "fabric",
    "azure fabric",
    "databricks",
    "azure databricks",
    "delta lake",
    "lakehouse",
    "etl",
    "elt",
    "data pipeline",
    "data pipelines",
    "data integration",
    "data warehouse",
    "data lake",
    "data lakehouse",
    "orchestration",
    "airflow",
    "apache airflow",
    "kafka",
    "apache kafka",
    "postgresql",
    "postgres",
    "sql server",
    "mysql",
    "pandas",
    "data analysis",
    "data modeling",
    "data quality"
]


preferred_locations = [
    "dominican republic",
    "republica dominicana",
    "república dominicana",
    "santo domingo",
    "latam",
    "latin america",
    "latin-america",
    "latinoamerica",
    "latinoamérica",
    "latino america",
    "latino américa",
    "américa latina",
    "america latina",
    "mexico",
    "méxico",
    "colombia",
    "argentina",
    "chile",
    "peru",
    "brazil",
    "brasil",
    "costa rica",
    "panama",
    "europe",
    "europa",
    "spain",
    "españa",
    "remote",
    "remoto",
    "remote work",
    "trabajo remoto",
    "fully remote",
    "100% remote",
    "work from home",
    "teletrabajo",
    "anywhere",
    "worldwide",
    "global",
    "remote worldwide"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Sumamos los puntos de match de `title` y `remote`
# En este caso como title es de tipo string y remote de tipo boolean no es necesario hacer muchas transformaciones.

# CELL ********************

title_pattern = "|".join(
    re.escape(title) for title in target_titles
)

df_matches = df_jobs.withColumn(
    "match_score",
    F.when(
        F.lower(F.col("title")).rlike(title_pattern),
        30
    ).otherwise(0)
    +
    F.when(
        F.col('remote'),
        20
    ).otherwise(0)
    -
    F.when(
        F.lower(F.col('title')).rlike(r'\bsenior\b|\bsr\.?\b'),
        20
    ).otherwise(0)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Creamos una columna para las skills que hicieron match y sumamos los puntos a `match_score`

# MARKDOWN ********************

# - Cada skill que haga match son 5 puntos que se sumarán a `match_score`
# - 30 es el número máximo de puntos que puede aportar 

# CELL ********************

target_skills_array = F.array(
    *[F.lit(skill) for skill in target_skills]
)

df_matches = df_matches.withColumn(
    "matched_skills",
    F.array_intersect(
        F.col('skills'),
        target_skills_array 
    )
).withColumn(
    "match_score",
    F.col("match_score") +
    F.least(
        F.size(F.col("matched_skills")) * 5,
        F.lit(30)
    )
).withColumn(
    "matched_skills",
    F.when(
        F.size(F.col("matched_skills")) == 0,
        F.array(F.lit("No match"))
    ).otherwise(F.col("matched_skills"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Sumamos el puntaje de location a `match_score`
# - Cada location que haga match son 5 puntos que se sumarán a `match_score`
# - 20 es el número máximo de puntos que puede aportar 

# CELL ********************

preferred_locations_array = F.array(
    *[F.lit(location) for location in preferred_locations]
)

match_locations = F.array_intersect(
    F.transform(
        F.col("location"),
        lambda x: F.lower(F.trim(x))
    ),
    preferred_locations_array
)

df_matches = df_matches.withColumn(
    'match_score',
    F.col('match_score') +
    F.least(
        F.size(match_locations)* 5,
        F.lit(20)
    )
        
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_matches.orderBy(F.col('match_score').desc()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ###

# MARKDOWN ********************

# ### Selecionamos los empleos con un mayor `match_score`
# En este caso tomaremos los empleos con un `match_score` >= 60

# CELL ********************

df_high_score_jobs = df_matches.filter(
    F.col('match_score') >= 60
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Creamos una columna para almacenar el identificador, en este caso un hash

# CELL ********************

df_high_score_jobs = df_high_score_jobs.withColumn(
    'hash_id',
    F.md5(
        F.concat_ws(
            "||", F.col('title'), F.col('company'), F.col('url')
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Agregamos la nueva columna en la primera posición

# CELL ********************

df_high_score_jobs = df_high_score_jobs.select('hash_id', *[c for c in df_high_score_jobs.columns if c != 'hash_id'])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_high_score_jobs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Guardamos los empleos en una tabla lista para ser consumida por la capa Gold

# CELL ********************

df_high_score_jobs.write.saveAsTable(
    'Silver.job_matches',
    mode='overwrite',
    format='delta'
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
