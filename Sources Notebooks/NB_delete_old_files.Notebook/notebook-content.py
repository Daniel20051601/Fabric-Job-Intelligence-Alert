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

# ### Elimina los archivos `.json` almacenados el bronze que tienen una semana de antiguedad

# CELL ********************

from datetime import datetime, timedelta
from notebookutils import mssparkutils 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

folder_paths = [
    'Files/Bronze/GetOnBoard',
    'Files/Bronze/Remotive'
]

threshold_date = datetime.now() - timedelta(days=7)

for folder in folder_paths:
    files = mssparkutils.fs.ls(folder)
    for file in files:
        file_mtime = datetime.fromtimestamp(file.modifyTime / 1000)
        if file_mtime < threshold_date:
            mssparkutils.fs.rm(file.path, True)
            print(f"Eliminado [{folder}]: {file.name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
