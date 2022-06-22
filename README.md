# DatabricksAndAzureSQL

### Sink: Azure SQL DATABASE
### Objective: Get data from a url and insert to a azure sql
### Author:  Hector Oviedo
### Creation Date: 2022-06-22

### --------------------------------------
### components:
#####   1-sqldatabricks:Azure Databricks Service 
#####   2-sqldatabricksaccount : Storage account
#####   3-sqldatabricksetl : SQL server
#####   4-sqldatabrickskeyvault : Key vault
#####   5-unificado (sqldatabricksetl/unificado) : azure sql database

### --------------------------------------

### notebooks:
#####   00.00-functions : functions and utils to import and reuse
#####   00.01-getDataAndWrite: main notebook which execute the functions getting the data and inserting into a database
#####    00.02-DataQuality: use great expectations and pyteams to data quality control


