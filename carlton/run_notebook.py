# Databricks notebook source
import sys

from pyspark.sql import SparkSession

from carlton.ingest.table import read, save
from carlton.utils.logger import log_error, log_info
from carlton.utils.spark_session_manager import SparkSessionManager


# COMMAND ----------

def process_args(args):
    """
    Processa os argumentos passados para a função.\n
    Processes the arguments passed to the function.

    Args:
        args (list): Lista de argumentos.
                     List of arguments.

    Returns:
        dict: Dicionário de propriedades raiz.
              Dictionary of root properties.
    """
    root_properties = {}
    for i, arg in enumerate(args):
        if arg.startswith('-'):
            root_properties[arg.replace('-', '')] = args[i + 1]
    return root_properties


# COMMAND ----------

log_info('Ingestão iniciada')
# Data ingestion started

# Processa os argumentos
# Process the arguments
args = ['ingest', '-function', 'ingest', '-storage_name_src', 'stadrisk', '-container_src', 'ctrdriskraw', '-file_resource', 'adls', '-type_run', 'batch', '-storage_name_tgt', 'stadrisk', '-container_tgt', 'dtmaster-catalog', '-schema_name', 'bronze', '-table_name', 'table_test', '-file_extension', 'csv', '-path_src', 'table_test', '-file_header', 'true', '-file_delimiter', ',']
print(args)
root_properties = process_args(args)

# Imprime as propriedades raiz
# Print the root properties
for p in root_properties:
    log_info(f'{p}: {root_properties[p]}')

# Criação de SparkSession
# Create SparkSession
spark = SparkSessionManager.create_spark_session('Carlton Ingest APP')

# COMMAND ----------

df = read(spark, root_properties)

# COMMAND ----------


# Leitura de dados
# Read data


display(df)

