# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import *
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.functions import col,lit

# COMMAND ----------

def GetDataFromUrl(url:str) -> pyspark.sql.dataframe.DataFrame:
    "Return a dataframe from a url"
    
    try:
        df_url =spark.createDataFrame(pd.read_csv(url)) 
        df_url = df_url.withColumn('FECHA_COPIA', current_timestamp()) 
    except Exception as e:
        dbutils.notebook.exit(f"error getting the dataframe from url because {e}")
    
    return df_url
  
def GetDataFromSQL(db :str , table: str) -> pyspark.sql.dataframe.DataFrame: 
    """
    Arguments:
        db: dataframe source , an str
        b: table source , an str
    Returns:
        pyspark dataframe from sql table
    """
    
    try:
        jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={db};user={jdbcUsername};password={jdbcPassword}"
        df_sql= spark.read.format("jdbc").option('url', jdbcUrl).option("dbtable",table).load()
        df_sql = df_sql.cache()
    except Exception as e:
        dbutils.notebook.exit(f"error getting the dataframe from sql because {e}") 
        
    return df_sql


def writeToSQL(db :str , table: str , dff , mode:str):
    """
    Arguments:
        db: dataframe destination , an str
        b: table source , an str
    Returns:
        pyspark dataframe from sql table
    """
    try:
        jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={db};user={jdbcUsername};password={jdbcPassword}"
        dff.write.format("jdbc") \
          .mode(mode) \
          .option("url", jdbcUrl) \
          .option("dbtable", table) \
          .save()
    except Exception as e:
        dbutils.notebook.exit(f"error writting dataframe to sql because {e}") 
