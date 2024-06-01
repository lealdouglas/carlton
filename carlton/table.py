from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date

from carlton.config_ingest import config_ingest_src, config_ingest_tgt
from carlton.helper import carlton_log, validate_args


def read(
    spark: SparkSession,
    config_ingest: dict,
    autoloader_config: dict,
    custom_config_spark={},
) -> DataFrame:

    try:

        autoloader_config = config_ingest_src(
            config_ingest, custom_config_spark
        )

        carlton_log(
            'configuracoes usadas na leitura: ', msg_dict=autoloader_config
        )

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
        carlton_log(str(e))


def save(
    spark: SparkSession,
    df: DataFrame,
    config_ingest: dict,
    custom_config_spark={},
):

    try:
        validate_args(
            ['schema_name', 'table_name', 'table_path', 'type_run'],
            config_ingest,
        )

        save_config = config_ingest_tgt(config_ingest, custom_config_spark)

        carlton_log('configuracoes usadas na escrita: ', msg_dict=save_config)

        lst_builtin = ['_rescued', 'carlton_current_date', 'carlton_metadata']
        columns_file = ', '.join(
            f'{col.lower()} STRING'
            for col in df.columns
            if col not in lst_builtin
        )

        carlton_log(
            f"cadastrando {config_ingest['table_name']} no schema: {config_ingest['schema_name']}"
        )
        carlton_log(
            f'utilizando liquid_cluster. Gerenciando pela coluna carlton_current_date'
        )

        query_create_table = f"""
            CREATE TABLE IF NOT EXISTS {config_ingest['schema_name']}.{config_ingest['table_name']} (
            {columns_file}
            ,_rescued STRING
            ,carlton_current_date DATE
            ,carlton_metadata struct<file_path:string,file_name:string,file_size:bigint,file_block_start:bigint,file_block_length:bigint,file_modification_time:timestamp>
            )
            USING DELTA CLUSTER BY (carlton_current_date) LOCATION '{config_ingest['table_path']}'"""

        carlton_log(f'cadastrando tabela: {query_create_table}')
        spark.sql(query_create_table)
        carlton_log(
            f"tabela {config_ingest['schema_name']}.{config_ingest['table_name']} criada com sucesso"
        )

        type_trigger = {}
        if config_ingest['type_run'] == 'batch':
            carlton_log(f'Identificando execucao como batch')
            type_trigger['availableNow'] = True

        else:
            carlton_log(f'Identificando execucao como stream')
            validate_args(['processingTime'], config_ingest)
            type_trigger['processingTime'] = config_ingest['processingTime']

        carlton_log(f'iniciando gravacao dos registros')

        df.writeStream.options(**save_config).outputMode('append').trigger(
            **type_trigger
        ).table(
            f"{config_ingest['schema_name']}.{config_ingest['table_name']}"
        ).awaitTermination()

        carlton_log(
            f"{config_ingest['schema_name']}.{config_ingest['table_name']} processada com sucesso"
        )

    except Exception as e:
        carlton_log(str(e))
