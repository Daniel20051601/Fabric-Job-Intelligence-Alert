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

# # 📥 Ingestión de ofertas de empleo de Remotive
# 
# Obtiene ofertas de trabajo desde la API de Remotive y las almacena en la capa Bronze del Lakehouse.

# MARKDOWN ********************

# ## Importamos las librerías necesarias 

# CELL ********************

import requests
import json
from datetime import datetime
import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Obtenemos los datos desde la Api de Remotive
# **Documentación:** https://github.com/remotive-com/remote-jobs-api


# CELL ********************

url = "https://remotive.com/api/remote-jobs"

params = {
    "search" : "Data Engineer"
}

response = requests.get(url = url, params = params)

data = response.json()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Visualizamos los empleos obtenidos

# CELL ********************

jobs = data["jobs"]


df = spark.createDataFrame(jobs)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Guardamos el archivo `.json` con los empleos
# Creamos las carpetas si no existen en el Lakehouse y guardamos el archivo


# CELL ********************

timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

dir_path = "/lakehouse/default/Files/Bronze/Remotive"
file_path = f"{dir_path}/remotive_jobs_{timestamp}.json"

os.makedirs(dir_path, exist_ok=True)

with open(file_path, "w", encoding="utf-8") as file:
  json.dump(jobs, file, ensure_ascii=False, indent=4)

print(f"Archivo guardado exitosamente en: {file_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
