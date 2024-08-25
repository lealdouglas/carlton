from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date

from ingest.config_ingest import config_ingest_src, config_ingest_tgt
from utils.helper import validate_args
from ingest.table import read, save
from utils.logger import log_error, log_info

# config_ingest: dict, custom_config_spark={}
def run() -> DataFrame:

    try:

        log_info(f'ingestao iniciada')

        spark = SparkSession.builder.appName(
            'Carlton Ingest APP'
        ).getOrCreate()

        print('Hello World!')

        # save(
        #     spark,
        #     read(spark, config_ingest, custom_config_spark),
        #     config_ingest,
        #     custom_config_spark,
        # )

        log_info(f'ingestao finalizada')

    except Exception as e:
        log_error(str(e))


if __name__ == '__main__':
    run()