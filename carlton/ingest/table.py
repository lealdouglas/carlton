from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date

from ingest.config_ingest import config_ingest_src, config_ingest_tgt
from utils.helper import validate_args
from utils.logger import log_error, log_info


def read(
    config_ingest: dict,
    autoloader_config: dict,
    custom_config_spark={},
) -> DataFrame:
    """
    Lê dados de uma fonte especificada usando as configurações fornecidas.

    Args:
        config_ingest (dict): Configurações de ingestão.
        autoloader_config (dict): Configurações do autoloader.
        custom_config_spark (dict, optional): Configurações personalizadas do Spark.

    Returns:
        DataFrame: DataFrame contendo os dados lidos.
    """
    try:
        # Configura o autoloader com as configurações de ingestão
        autoloader_config = config_ingest_src(
            config_ingest, custom_config_spark
        )

        # Loga as configurações usadas na leitura
        log_info(
            'configuracoes usadas na leitura: ', msg_dict=autoloader_config
        )

        # Cria ou obtém uma sessão Spark
        spark = SparkSession.builder.getOrCreate()

        # Lê os dados da fonte especificada e retorna um DataFrame
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
        # Loga qualquer erro que ocorra durante a leitura
        log_error(str(e))


def save(
    df: DataFrame,
    config_ingest: dict,
    custom_config_spark={},
):
    """
    Salva os dados do DataFrame em um destino especificado usando as configurações fornecidas.

    Args:
        df (DataFrame): DataFrame contendo os dados a serem salvos.
        config_ingest (dict): Configurações de ingestão.
        custom_config_spark (dict, optional): Configurações personalizadas do Spark.
    """
    try:
        # Valida os argumentos necessários
        validate_args(
            ['schema_name', 'table_name', 'table_path', 'type_run'],
            config_ingest,
        )

        # Configura o destino com as configurações de ingestão
        save_config = config_ingest_tgt(config_ingest, custom_config_spark)

        # Loga as configurações usadas na escrita
        log_info('configuracoes usadas na escrita: ', msg_dict=save_config)

        # Cria ou obtém uma sessão Spark
        spark = SparkSession.builder.getOrCreate()

        # Lista de colunas internas que não devem ser incluídas na tabela
        lst_builtin = ['_rescued', 'carlton_current_date', 'carlton_metadata']
        columns_file = ', '.join(
            f'{col.lower()} STRING'
            for col in df.columns
            if col not in lst_builtin
        )

        # Loga informações sobre a tabela que está sendo criada
        log_info(
            f"cadastrando tabela {config_ingest['table_name']} no schema {config_ingest['schema_name']}"
        )
        log_info(
            f'utilizando liquid_cluster. Gerenciando pela coluna carlton_current_date'
        )

        # Cria a tabela no destino especificado
        query_create_table = f"""
            CREATE TABLE IF NOT EXISTS {config_ingest['schema_name']}.{config_ingest['table_name']} (
            {columns_file}
            ,_rescued STRING
            ,carlton_current_date DATE
            ,carlton_metadata struct<file_path:string,file_name:string,file_size:bigint,file_block_start:bigint,file_block_length:bigint,file_modification_time:timestamp>
            )
            USING DELTA CLUSTER BY (carlton_current_date) LOCATION '{config_ingest['table_path']}'"""

        log_info(f'cadastrando tabela: {query_create_table}')
        spark.sql(query_create_table)
        log_info(
            f"tabela {config_ingest['schema_name']}.{config_ingest['table_name']} criada com sucesso"
        )

        # Configura o tipo de trigger para a gravação dos dados
        type_trigger = {}
        if config_ingest['type_run'] == 'batch':
            log_info(f'Identificando execucao como batch')
            type_trigger['availableNow'] = True
        else:
            log_info(f'Identificando execucao como stream')
            validate_args(['trigger_processing_time'], config_ingest)
            type_trigger['processingTime'] = config_ingest[
                'trigger_processing_time'
            ]

        log_info(f'iniciando gravacao dos registros')

        # Grava os dados no destino especificado
        df.writeStream.options(**save_config).outputMode('append').trigger(
            **type_trigger
        ).table(
            f"{config_ingest['schema_name']}.{config_ingest['table_name']}"
        ).awaitTermination()

        log_info(
            f"{config_ingest['schema_name']}.{config_ingest['table_name']} processada com sucesso"
        )

    except Exception as e:
        # Loga qualquer erro que ocorra durante a gravação
        log_error(str(e))
