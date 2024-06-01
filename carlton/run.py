from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date

from carlton.config_ingest import config_ingest_src, config_ingest_tgt
from carlton.helper import carlton_log, validate_args
from carlton.table import read, save


def run(config_ingest: dict, custom_config_spark={}) -> DataFrame:

    try:

        carlton_log(f'ingestao iniciada')

        spark = SparkSession.builder.appName(
            'Carlton Ingest APP'
        ).getOrCreate()

        save(
            spark,
            read(spark, config_ingest, custom_config_spark),
            config_ingest,
            custom_config_spark,
        )

        carlton_log(f'ingestao finalizada')

    except Exception as e:
        carlton_log(str(e))
