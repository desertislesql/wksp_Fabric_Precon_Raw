# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ce1a42f7-5c62-4cbe-834d-ca1f4e0bab68",
# META       "default_lakehouse_name": "gpgOpDataStore",
# META       "default_lakehouse_workspace_id": "f097d258-1737-4f0b-8e09-09197805d11b",
# META       "known_lakehouses": [
# META         {
# META           "id": "ce1a42f7-5c62-4cbe-834d-ca1f4e0bab68"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Transforming Iowa Liquor Sales

# MARKDOWN ********************

# examining liquor content

# CELL ********************

df = spark.sql("SELECT * FROM gpgOpDataStore.dbo.iowa_liquor_sales LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
