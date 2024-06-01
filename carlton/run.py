from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date

from carlton.config_ingest import config_ingest_src, config_ingest_tgt
from carlton.helper import carlton_log, validate_args
from carlton.save import save


def run(config_ingest: dict, custom_config_spark={}) -> DataFrame:

    try:
        spark = SparkSession.builder.appName(
            'Carlton Ingest APP'
        ).getOrCreate()

        autoloader_config = config_ingest_src(config_ingest)
        save_config = config_ingest_tgt(config_ingest)

        carlton_log('autoloader_config', msg_dict=autoloader_config)
        carlton_log('save_config', msg_dict=save_config)

        df = (
            spark.readStream.format('cloudFiles')
            .options(**config_ingest_src(config_ingest, custom_config_spark))
            .load(config_ingest['file_path'])
            .select(
                '*',
                current_date().alias('carlton_current_date'),
                col('_metadata').alias('carlton_metadata'),
            )
        )

        carlton_log('estrutura do arquivo origem', str(df.printSchema()))

        save(df, config_ingest)

    except Exception as e:
        carlton_log(e)
