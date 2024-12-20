# table.py
from pyspark.sql import DataFrame, SparkSession

from carlton.ingest.data_reader import DataReader
from carlton.ingest.data_saver import DataSaver
from carlton.utils.logger import log_error, log_info


def read(
    spark: SparkSession, config_ingest: dict, custom_config_spark={}
) -> DataFrame:
    """
    Função wrapper para ler dados usando DataReader.\n
    Wrapper function to read data using DataReader.

    Args:
        config_ingest (dict): Configurações de ingestão.
                              Ingestion configurations.
        custom_config_spark (dict, optional): Configurações personalizadas do Spark.
                                              Custom Spark configurations.

    Returns:
        DataFrame: DataFrame contendo os dados lidos.
                   DataFrame containing the read data.
    """
    try:
        log_info('DataReader.read_data')
        return DataReader.read_data(spark, config_ingest, custom_config_spark)
    except Exception as e:
        log_error(f'Erro na função read: {str(e)}')
        raise


def save(
    spark: SparkSession,
    df: DataFrame,
    config_ingest: dict,
    custom_config_spark={},
) -> None:
    """
    Função wrapper para salvar dados usando DataSaver.\n
    Wrapper function to save data using DataSaver.

    Args:
        df (DataFrame): DataFrame contendo os dados a serem salvos.
                        DataFrame containing the data to be saved.
        config_ingest (dict): Configurações de ingestão.
                              Ingestion configurations.
        custom_config_spark (dict, optional): Configurações personalizadas do Spark.
                                              Custom Spark configurations.
    """
    try:
        log_info('DataSaver.save_data')
        DataSaver.save_data(spark, df, config_ingest, custom_config_spark)
    except Exception as e:
        log_error(f'Erro na função save: {str(e)}')
        raise
