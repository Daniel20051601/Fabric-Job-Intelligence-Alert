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

# # 📥Ingestión de ofertas de empleo de Get On Board

# MARKDOWN ********************

# ### Configuración y dependencias

# CELL ********************

import requests
import json
from pyspark.sql import functions as F
from datetime import datetime
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Obtenemos los datos desde la api
# **Dcoumentación:** https://www.getonbrd.com/api-doc.html#description/introduction

# MARKDOWN ********************

# #### 1. Configuramos el cuerpo de la petición

# CELL ********************

base_url = "https://www.getonbrd.com"
endpoint = "/api/v0/categories/3/jobs"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

params={
    "per_page": 100,
    "lang":"es",
    "expand": '["company", "tags"]'
}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 2. Obtenemos los datos de la api

# CELL ********************

response = requests.get(base_url + endpoint, headers=headers, params=params)
response.raise_for_status()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 3. Extracción del payload JSON
# Extraemos la lista de ofertas contenida en el campo `data` de la respuesta devuelta por la API.

# CELL ********************

jobs = response.json().get("data", [])
jobs


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Almacenamos el archivo `.json` con los empleos

# CELL ********************

timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

dir_path = '/lakehouse/default/Files/Bronze/GetOnBoard'
file_path = f'{dir_path}/getonboard_jobs_{timestamp}.json'

os.makedirs(dir_path, exist_ok=True)

with open(file_path, 'w', encoding='utf-8') as file:
    json.dump(jobs, file, ensure_ascii=False, indent=4)

print(f'Archivo guardado exitosamente en {file_path}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Solucion que debe ser movida a Silver

# CELL ********************

df_jobs = spark.read.json(spark.sparkContext.parallelize([json.dumps(j) for j in jobs]))
display(df_jobs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Almacenamos el archivo `.json` con los empleos

# MARKDOWN ********************

# # 

# CELL ********************

df_jobs = df_jobs.select(
    F.col("id").alias("job_id"),
    F.col("attributes.title").alias("title"),
    F.col("attributes.company.data.id").alias("company_id"),
    F.col("attributes.category_name").alias("category"),
    F.col("attributes.published_at").cast("long").alias("published_at_ts"),
    F.col("attributes.remote").cast("boolean").alias("remote"),
    F.col("attributes.remote_modality").alias("remote_modality"),
    F.col("attributes.countries").alias("location"),
    F.col("attributes.min_salary").cast("double").alias("min_salary"),
    F.col("attributes.max_salary").cast("double").alias("max_salary"),
    F.col("attributes.tags.data.id").alias('skills'),
    F.col("attributes.description").alias("description"),
    F.col("attributes.functions").alias("functions"),
    F.col("attributes.desirable").alias("desirable"),
    F.col("attributes.perks").alias("perks"),
    F.col("links.public_url").alias("url")
)

display(df_jobs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

company_ids = [row.company_id for row in df_jobs.select("company_id").distinct().dropna().collect()]

companies_data = []
for cid in company_ids:
    try:
        res = requests.get(f"https://www.getonbrd.com/api/v0/companies/{cid}", headers=headers)
        if res.status_code == 200:
            c_json = res.json().get("data", {})
            companies_data.append({
                "company_id": str(cid),
                "company_name": c_json.get("attributes", {}).get("name")
            })
    except Exception:
        continue

df_companies = spark.createDataFrame(companies_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_jobs = (
    df_jobs
    .join(df_companies, on="company_id", how="left")
    .withColumn("company", F.coalesce(F.col("company_name"), F.col("company_id")))
    .drop('company_name', 'company_id')
)

display(df_jobs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
