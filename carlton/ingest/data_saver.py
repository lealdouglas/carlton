# data_saver.py
from pyspark.sql import DataFrame, SparkSession

from carlton.ingest.config_ingestor import ConfigIngestor
from carlton.ingest.config_validator import ConfigValidator
from carlton.utils.logger import log_error, log_info


class DataSaver:
    @staticmethod
    def save_data(
        spark: SparkSession,
        df: DataFrame,
        config_ingest: dict,
        custom_config_spark={},
    ) -> None:
        """
        Salva os dados do DataFrame em um destino especificado usando as configurações fornecidas.
        Saves the DataFrame data to a specified destination using the provided configurations.

        Args:
            df (DataFrame): DataFrame contendo os dados a serem salvos.
                            DataFrame containing the data to be saved.
            config_ingest (dict): Configurações de ingestão.
                                  Ingestion configurations.
            custom_config_spark (dict, optional): Configurações personalizadas do Spark.
                                                  Custom Spark configurations.

        Example:
            >>> from pyspark.sql import SparkSession
            >>> from data_saver import DataSaver
            >>> spark = SparkSession.builder.appName("example").getOrCreate()
            >>> data = [("Alice", 1), ("Bob", 2)]
            >>> columns = ["name", "value"]
            >>> df = spark.createDataFrame(data, columns)
            >>> config_ingest = {'schema_name': 'default','table_name': 'example_table','table_path': '/path/to/table','type_run': 'batch','trigger_processing_time': '10 seconds'}
            >>> custom_config_spark = {'spark.some.config.option': 'some-value'}
            >>> DataSaver.save_data(df, config_ingest, custom_config_spark)
        """
        try:
            # Valida os argumentos necessários
            # Validate the necessary arguments
            ConfigValidator.validate_args(
                ['schema_name', 'table_name', 'table_path', 'type_run'],
                config_ingest,
            )

            # Configura o destino com as configurações de ingestão
            # Configure the destination with the ingestion configurations
            save_config = ConfigIngestor.config_ingest_tgt(
                config_ingest, custom_config_spark
            )

            # Registra as configurações usadas para escrita
            # Log the configurations used for writing
            log_info(f'Configurations used for writing: {save_config}')

            # Lista de colunas internas que não devem ser incluídas na tabela
            # List of internal columns that should not be included in the table
            lst_builtin = [
                '_rescued',
                'carlton_current_date',
                'carlton_metadata',
            ]
            columns_file = ', '.join(
                f'{col.lower()} STRING'
                for col in df.columns
                if col not in lst_builtin
            )

            # Registra informações sobre a tabela sendo criada
            # Log information about the table being created
            log_info(
                f"Registering table {config_ingest['table_name']} in schema {config_ingest['schema_name']}"
            )
            log_info(
                f'Using liquid_cluster. Managed by the column carlton_current_date'
            )

            # Cria a tabela no destino especificado
            # Create the table in the specified destination
            query_create_table = f"""CREATE TABLE IF NOT EXISTS {config_ingest['schema_name']}.{config_ingest['table_name']} ({columns_file},_rescued STRING,carlton_current_date DATE,carlton_metadata struct<file_path:string,file_name:string,file_size:bigint,file_block_start:bigint,file_block_length:bigint,file_modification_time:timestamp>) USING DELTA CLUSTER BY (carlton_current_date) LOCATION '{config_ingest['table_path']}'"""

            log_info(f'Registering table: {query_create_table}')
            spark.sql(query_create_table)
            log_info(
                f"Table {config_ingest['schema_name']}.{config_ingest['table_name']} created successfully"
            )

            # Configura o tipo de trigger para escrita dos dados
            # Configure the trigger type for writing the data
            type_trigger = {}
            if config_ingest['type_run'] == 'batch':
                log_info(f'Identifying execution as batch')
                type_trigger['availableNow'] = True
            else:
                log_info(f'Identifying execution as stream')
                ConfigValidator.validate_args(
                    ['trigger_processing_time'], config_ingest
                )
                type_trigger['processingTime'] = config_ingest[
                    'trigger_processing_time'
                ]

            log_info(f'Starting record writing')

            # Escreve os dados no destino especificado
            # Write the data to the specified destination
            df.writeStream.options(**save_config).outputMode('append').trigger(
                **type_trigger
            ).table(
                f"{config_ingest['schema_name']}.{config_ingest['table_name']}"
            ).awaitTermination()

            log_info(
                f"{config_ingest['schema_name']}.{config_ingest['table_name']} processed successfully"
            )

        except Exception as e:
            # Registra qualquer erro que ocorra durante a escrita
            # Log any error that occurs during writing
            log_error(str(e))
