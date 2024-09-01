from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_date

from carlton.ingest.config_ingest import config_ingest_src, config_ingest_tgt
from carlton.ingest.table import read, save
from carlton.utils.helper import validate_args
from carlton.utils.logger import log_error, log_info
import sys

def run(args=sys.argv[1:]):
    """
    Função principal para executar a ingestão de dados.

    Args:
        args (list): Lista de argumentos passados para a função.

    Returns:
        DataFrame: DataFrame resultante da operação de ingestão.
    """
    try:
        # Cria um dicionário com as propriedades principais
        root_properties = {
            "modulo": args[0], # ingest ou quality
            "data": args[1] # data processamento
        }

        # Loga cada propriedade
        for key, value in root_properties.items():
            log_info(f"{key}: {value}")

        log_info('Ingestão iniciada')

        # Criação de SparkSession
        spark = SparkSession.builder.appName('Carlton Ingest APP').getOrCreate()

        print('Hello World!')

        # Exemplo de leitura e salvamento de dados (descomente se necessário)
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