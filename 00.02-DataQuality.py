# Databricks notebook source
# MAGIC %run "./00.00-functions"

# COMMAND ----------

from datetime import datetime, timezone
import pandas as pd
import great_expectations as ge
import great_expectations.jupyter_ux
from great_expectations.exceptions import DataContextError
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.types.base import DatasourceConfig
from great_expectations.data_context.types.base import FilesystemStoreBackendDefaults
from great_expectations.data_context import BaseDataContext
from pathlib import Path
from os.path import abspath
import requests
import json
import pymsteams

# COMMAND ----------

# DBTITLE 1,Data Context
project_path = Path(abspath('')).parent.absolute().as_posix()
project_path

datasource = "my_pandas_datasource"
context = BaseDataContext(
                project_config=DataContextConfig(
                    config_version=2,
                    plugins_directory=f"{project_path}/plugins",
                    datasources={
                        datasource: DatasourceConfig(
                            class_name="PandasDatasource",
                            batch_kwargs_generators={}
                        )
                    },
                    validation_operators={
                        "action_list_operator": {
                            "class_name": "ActionListValidationOperator",
                            "action_list": [
                                {
                                    "name": "store_validation_result",
                                    "action": {"class_name": "StoreValidationResultAction"},
                                },
                                {
                                    "name": "update_data_docs",
                                    "action": {"class_name": "UpdateDataDocsAction"},
                                },
                            ],
                        }
                    },
                    store_backend_defaults=FilesystemStoreBackendDefaults(
                        root_directory=project_path
                    )
                )
            )

# COMMAND ----------

# DBTITLE 1,Suite
expectation_suite_name = 'dataSQL'

try:
    suite = context.get_expectation_suite(expectation_suite_name)
except DataContextError:
    suite = context.create_expectation_suite(expectation_suite_name)
    
context.list_expectation_suite_names()

expectation_suite_name = "dataSQL"

# COMMAND ----------

# DBTITLE 1,getData
dfFromSQL = GetDataFromSQL(db= 'unificado' , table= 'dbo.Unificado')
df = dfFromSQL.toPandas()

# COMMAND ----------

# DBTITLE 1,create the batch object( data, suite and context)
batch_kwargs = {
    "datasource": "my_pandas_datasource",
    "dataset": df,
    "expectation_suite_names": expectation_suite_name
}

batch = context.get_batch(batch_kwargs, expectation_suite_name)
batch.head(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ### expectations

# COMMAND ----------

#fecha-copia se añade cada vez  que se suman nuevos datos por lo que no deberia ser nulos
batch.expect_column_values_to_not_be_null('FECHA_COPIA')

# COMMAND ----------

batch.expect_column_distinct_values_to_be_in_set('RESULTADO', ['A/A','A/C','A/G','A/T','C/A','C/C','C/G','C/T','G/A','G/C','G/G','G/T','T/A','T/C','T/G']) #'T/T

# COMMAND ----------

#obtenemos las expectativas falladas
sucess_expectations = batch.get_expectation_suite()
all_expectation = batch.get_expectation_suite(discard_failed_expectations=False)

for expectation in all_expectation["expectations"]:
    if  expectation not in sucess_expectations["expectations"]:
            print(expectation)
            
            # send failed expectations to teams
            myTeamsMessage = pymsteams.connectorcard("https://everisgroup.webhook.office.com/webhookb2/0117c156-c1e8-44c9-8551-fa3e7312aa6f@3048dc87-43f0-4100-9acb-ae1971c79395/IncomingWebhook/36c2b6de4713482e81ff43f24e6688dd/c8e43f27-5282-4826-bd7e-98577dbed2d9")
            myTeamsMessage.text(f'hay una falla de calidad de datos en la columna {expectation["kwargs"]["column"]}')
            myTeamsMessage.send()
            
