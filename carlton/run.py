import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date

from carlton.ingest.config_ingest import config_ingest_src, config_ingest_tgt
from carlton.ingest.table import read, save
from carlton.mock.eh_send_message import exec
from carlton.utils.helper import validate_args
from carlton.utils.logger import log_error, log_info


def run(args=sys.argv[1:]):
    """
    Função principal para executar a ingestão de dados.

    Args:
        args (list): Lista de argumentos passados para a função.

    Returns:
        DataFrame: DataFrame resultante da operação de ingestão.
    """
    try:

        log_info('Ingestão iniciada')

        root_properties = {}
        for i, arg in enumerate(args):        
            if arg.startswith('-'):
                root_properties[arg.replace('-', '')] = args[i+1]

        for p in root_properties:
            print(p, root_properties[p])

        print('Hello World!')

        # Criação de SparkSession
        spark = SparkSession.builder.appName(
            'Carlton Ingest APP'
        ).getOrCreate()

        # Exemplo de leitura e salvamento de dados (descomente se necessário)

        df = (
            spark,
            read(spark, root_properties, custom_config_spark),
            root_properties,
            custom_config_spark,
        )

        # save(
        #     spark,
        #     read(spark, root_properties, custom_config_spark),
        #     root_properties,
        #     custom_config_spark,
        # )

        log_info('Ingestão finalizada')

    except Exception as e:
        # Loga qualquer erro que ocorrer
        log_error(str(e))
