from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date

from carlton.config_ingest import config_ingest_src
from carlton.helper import carlton_log, validate_args


def save(
    df: DataFrame,
    config_ingest: dict,
):

    try:
        validate_args(
            ['schema_name', 'table_name', 'table_path', 'type_run'],
            config_ingest,
        )

        spark = SparkSession.builder.getOrCreate()

        lst_builtin = ['_rescued', 'carlton_current_date', 'carlton_metadata']
        columns_file = ', '.join(
            f'{col.lower()} STRING'
            for col in df.columns
            if col not in lst_builtin
        )

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {config_ingest['schema_name']}.{config_ingest['table_name']} (
            {columns_file}
            ,_rescued STRING
            ,carlton_current_date DATE
            ,carlton_metadata struct<file_path:string,file_name:string,file_size:bigint,file_block_start:bigint,file_block_length:bigint,file_modification_time:timestamp>
            )
            USING DELTA CLUSTER BY (carlton_current_date) LOCATION '{config_ingest['table_path']}'
        """
        )

        type_trigger = {}
        if config_ingest['type_run'] == 'batch':
            type_trigger['availableNow'] = True

        else:
            validate_args(['processingTime'], config_ingest)
            type_trigger['processingTime'] = config_ingest['processingTime']

        df.writeStream.options(**save_config).outputMode('append').trigger(
            **type_trigger
        ).table(
            f"{config_ingest['schema_name']}.{config_ingest['table_name']}"
        )

        carlton_log(
            f"{config_ingest['schema_name']}.{config_ingest['table_name']} processada com sucesso"
        )

    except Exception as e:
        carlton_log(e)
