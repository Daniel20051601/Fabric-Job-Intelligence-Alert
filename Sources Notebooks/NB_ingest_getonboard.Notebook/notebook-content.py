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
    "per_page": 120,
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

# # 
