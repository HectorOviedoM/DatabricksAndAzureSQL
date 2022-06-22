# Databricks notebook source
# DBTITLE 1,imports
import pandas as pd
from pyspark.sql.functions import *
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.functions import col,lit

# COMMAND ----------

# DBTITLE 1,get functions
# MAGIC %run "./00.00-functions"

# COMMAND ----------

#get data from url
dfFromUrl = GetDataFromUrl('https://adlschallenge.blob.core.windows.net/challenge/nuevas_filas.csv?sp=r&st=2022-06-20T14:51:53Z&se=2022-12-31T22:51:53Z&spr=https&sv=2021-06-08&sr=b&sig=y9hLJFCVvGh1Ej58SXqsXTSVC6ABoVuQgfECDOd83Lw%3D')
print((dfFromUrl.count(), len(dfFromUrl.columns)))

# COMMAND ----------

#write data to sql
writeToSQL(db= 'unificado' , table= 'Unificado', dff = dfFromUrl, mode='append')

# COMMAND ----------

#get data from sql
dfFromSQL = GetDataFromSQL(db= 'unificado' , table= 'dbo.Unificado')
print((dfFromSQL.count(), len(dfFromSQL.columns)))

# COMMAND ----------

#drop duplicates using windows functiosn, partitionin by id, mustra and resultado , and get the first value from each partition(first are the last 'fecha_copia' because orderby desc)
df_withoutDuplicates = dfFromSQL.withColumn("row_number",F.row_number()\
                               .over(Window.partitionBy(dfFromSQL.ID, dfFromSQL.MUESTRA,dfFromSQL.RESULTADO)\
                               .orderBy(dfFromSQL.FECHA_COPIA.desc())))\
                               .filter(F.col("row_number")==1).drop("row_number")

print((df_withoutDuplicates.count(), len(df_withoutDuplicates.columns)))
df_withoutDuplicates = df_withoutDuplicates.cache()

# COMMAND ----------

#write data without duplicates to sql
writeToSQL(db= 'unificado' , table= 'Unificado', dff = df_withoutDuplicates, mode= 'overwrite')

# COMMAND ----------

#write a full original table
#df_from_databricks = spark.sql("select * from datos")
#print((df_from_databricks.count(), len(df_from_databricks.columns)))
#writeToSQL(db= 'unificado' , table= 'Unificado', dff = df_from_databricks, mode='overwrite')
