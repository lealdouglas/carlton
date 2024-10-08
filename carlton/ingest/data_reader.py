# data_reader.py
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date

from carlton.ingest.config_ingestor import ConfigIngestor
from carlton.utils.logger import log_error, log_info


class DataReader:
    @staticmethod
    def read_data(
        spark: SparkSession, config_ingest: dict, custom_config_spark={}
    ) -> DataFrame:
        """
        Lê dados de uma fonte especificada usando as configurações fornecidas.
        Reads data from a specified source using the provided configurations.

        Args:
            config_ingest (dict): Configurações de ingestão.
                                  Ingestion configurations.
            custom_config_spark (dict, optional): Configurações personalizadas do Spark.
                                                  Custom Spark configurations.

        Returns:
            DataFrame: DataFrame contendo os dados lidos.
                       DataFrame containing the read data.

        Example:
            >>> config_ingest = {'type_run': 'batch','file_extension': 'csv','file_resource': 'adls','container_src': 'source_container','storage_name_src': 'source_storage','container_tgt': 'target_container','storage_name_tgt': 'target_storage','path_src': 'source_path','file_header': 'true','file_delimiter': ',','schemaLocation': 'schema_location','carlton_file_path': 'path/to/data'}
            >>> custom_config_spark = {'spark.some.config.option': 'some-value'}
            >>> df = DataReader.read_data(config_ingest, custom_config_spark)
        """
        try:
            # Configura o autoloader com as configurações de ingestão
            # Configure the autoloader with the ingestion configurations
            log_info('Configura o autoloader com as configurações de ingestão')
            autoloader_config = ConfigIngestor.config_ingest_src(
                config_ingest, custom_config_spark
            )

            # Registra as configurações usadas para leitura
            # Log the configurations used for reading
            log_info(f'Configurations used for reading: {autoloader_config}')

            log_info(f"load file from {config_ingest['carlton_file_path']}")

            # Lê os dados da fonte especificada e retorna um DataFrame
            # Read the data from the specified source and return a DataFrame
            return (
                spark.readStream.format('cloudFiles')
                .options(**autoloader_config)
                .load(config_ingest['carlton_file_path'])
                .select(
                    '*',
                    current_date().alias('carlton_current_date'),
                    col('_metadata').alias('carlton_metadata'),
                )
            )

        except Exception as e:
            # Registra qualquer erro que ocorra durante a leitura
            # Log any error that occurs during reading
            log_error(str(e))
            return None
