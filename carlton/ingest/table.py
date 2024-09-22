# table.py
from data_reader import DataReader
from data_saver import DataSaver
from pyspark.sql import DataFrame


def read(config_ingest: dict, custom_config_spark={}) -> DataFrame:
    """
    Função wrapper para ler dados usando DataReader.
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
    return DataReader.read_data(config_ingest, custom_config_spark)


def save(df: DataFrame, config_ingest: dict, custom_config_spark={}) -> None:
    """
    Função wrapper para salvar dados usando DataSaver.
    Wrapper function to save data using DataSaver.

    Args:
        df (DataFrame): DataFrame contendo os dados a serem salvos.
                        DataFrame containing the data to be saved.
        config_ingest (dict): Configurações de ingestão.
                              Ingestion configurations.
        custom_config_spark (dict, optional): Configurações personalizadas do Spark.
                                              Custom Spark configurations.
    """
    DataSaver.save_data(df, config_ingest, custom_config_spark)
