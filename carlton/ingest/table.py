# data_reader.py
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date
from carlton.ingest.config_ingest import config_ingest_src
from carlton.utils.logger import log_error, log_info

class DataReader:
    @staticmethod
    def read_data(config_ingest: dict, custom_config_spark={}) -> DataFrame:
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
        """
        try:
            # Configura o autoloader com as configurações de ingestão
            # Configure the autoloader with the ingestion configurations
            autoloader_config = config_ingest_src(config_ingest, custom_config_spark)

            # Registra as configurações usadas para leitura
            # Log the configurations used for reading
            log_info('Configurations used for reading: ', msg_dict=autoloader_config)

            # Cria ou obtém uma sessão Spark
            # Create or get a Spark session
            spark = SparkSession.builder.getOrCreate()

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

# data_saver.py
from pyspark.sql import DataFrame, SparkSession
from carlton.ingest.config_ingest import config_ingest_tgt
from carlton.utils.helper import validate_args
from carlton.utils.logger import log_error, log_info

class DataSaver:
    @staticmethod
    def save_data(df: DataFrame, config_ingest: dict, custom_config_spark={}) -> None:
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
        """
        try:
            # Valida os argumentos necessários
            # Validate the necessary arguments
            validate_args(['schema_name', 'table_name', 'table_path', 'type_run'], config_ingest)

            # Configura o destino com as configurações de ingestão
            # Configure the destination with the ingestion configurations
            save_config = config_ingest_tgt(config_ingest, custom_config_spark)

            # Registra as configurações usadas para escrita
            # Log the configurations used for writing
            log_info('Configurations used for writing: ', msg_dict=save_config)

            # Cria ou obtém uma sessão Spark
            # Create or get a Spark session
            spark = SparkSession.builder.getOrCreate()

            # Lista de colunas internas que não devem ser incluídas na tabela
            # List of internal columns that should not be included in the table
            lst_builtin = ['_rescued', 'carlton_current_date', 'carlton_metadata']
            columns_file = ', '.join(
                f'{col.lower()} STRING'
                for col in df.columns
                if col not in lst_builtin
            )

            # Registra informações sobre a tabela sendo criada
            # Log information about the table being created
            log_info(f"Registering table {config_ingest['table_name']} in schema {config_ingest['schema_name']}")
            log_info(f"Using liquid_cluster. Managed by the column carlton_current_date")

            # Cria a tabela no destino especificado
            # Create the table in the specified destination
            query_create_table = f"""
                CREATE TABLE IF NOT EXISTS {config_ingest['schema_name']}.{config_ingest['table_name']} (
                {columns_file}
                ,_rescued STRING
                ,carlton_current_date DATE
                ,carlton_metadata struct<file_path:string,file_name:string,file_size:bigint,file_block_start:bigint,file_block_length:bigint,file_modification_time:timestamp>
                )
                USING DELTA CLUSTER BY (carlton_current_date) LOCATION '{config_ingest['table_path']}'"""

            log_info(f'Registering table: {query_create_table}')
            spark.sql(query_create_table)
            log_info(f"Table {config_ingest['schema_name']}.{config_ingest['table_name']} created successfully")

            # Configura o tipo de trigger para escrita dos dados
            # Configure the trigger type for writing the data
            type_trigger = {}
            if config_ingest['type_run'] == 'batch':
                log_info(f'Identifying execution as batch')
                type_trigger['availableNow'] = True
            else:
                log_info(f'Identifying execution as stream')
                validate_args(['trigger_processing_time'], config_ingest)
                type_trigger['processingTime'] = config_ingest['trigger_processing_time']

            log_info(f'Starting record writing')

            # Escreve os dados no destino especificado
            # Write the data to the specified destination
            df.writeStream.options(**save_config).outputMode('append').trigger(
                **type_trigger
            ).table(
                f"{config_ingest['schema_name']}.{config_ingest['table_name']}"
            ).awaitTermination()

            log_info(f"{config_ingest['schema_name']}.{config_ingest['table_name']} processed successfully")

        except Exception as e:
            # Registra qualquer erro que ocorra durante a escrita
            # Log any error that occurs during writing
            log_error(str(e))

# spark_session_manager.py
from pyspark.sql import SparkSession

class SparkSessionManager:
    @staticmethod
    def get_spark_session() -> SparkSession:
        """
        Obtém ou cria uma sessão Spark.
        Get or create a Spark session.

        Returns:
            SparkSession: A sessão Spark.
                          The Spark session.
        """
        return SparkSession.builder.getOrCreate()

# table.py
from pyspark.sql import DataFrame
from data_reader import DataReader
from data_saver import DataSaver

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