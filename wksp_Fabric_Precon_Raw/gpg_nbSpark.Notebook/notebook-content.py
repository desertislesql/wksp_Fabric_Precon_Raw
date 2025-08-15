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

# CELL ********************

from pyspark.sql import SparkSession 
from pyspark.sql import SQLContext 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfiowa = spark.sql("SELECT * FROM gpgOpDataStore.dbo.iowa_liquor_sales LIMIT 1000")
display(dfiowa)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfdate = spark.sql("SELECT * FROM gpgOpDataStore.dbo.date LIMIT 1000")
display(dfdate)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfdate.createOrReplaceTempView("calendar")
dfiowa.createOrReplaceTempView("alcohol")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC Select a.DayNameofWeek, b.CategoryName, Count(*) as Sales
# MAGIC from calendar a 
# MAGIC JOIN alcohol b 
# MAGIC     on a.FullDateAlternateKey = b.Date
# MAGIC GROUP BY a.DayNameofWeek, b.CategoryName
# MAGIC   

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Persist the query
sqlContext = SQLContext(spark) 
sqlString = ("""Select a.DayNameofWeek, b.CategoryName, Count(*) as Sales
from calendar a 
JOIN alcohol b 
    on a.FullDateAlternateKey = b.Date
GROUP BY a.DayNameofWeek, b.CategoryName
""") 


dfsales = sqlContext.sql(sqlString) 

# Show the DataFrame 
display(dfsales) 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Write to the lakehouse
lakepath = "abfss://Precon_Raw@onelake.dfs.fabric.microsoft.com/gpgOpDataStore.Lakehouse/Tables/dbo/Sales" 
dfsales.write.format("delta").mode("overwrite").save(lakepath) 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
