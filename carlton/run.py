import sys

from pyspark.sql import SparkSession

from carlton.ingest.table import read, save
from carlton.utils.logger import log_error, log_info
from carlton.utils.spark_session_manager import SparkSessionManager


def process_args(args):
    """
    Processa os argumentos passados para a função.\n
    Processes the arguments passed to the function.

    Args:
        args (list): Lista de argumentos.
                     List of arguments.

    Returns:
        dict: Dicionário de propriedades raiz.
              Dictionary of root properties.
    """
    root_properties = {}
    for i, arg in enumerate(args):
        if arg.startswith('-'):
            root_properties[arg.replace('-', '')] = args[i + 1]
    return root_properties


def main(args=sys.argv[1:]):
    """
    Função principal para executar a ingestão de dados.\n
    Main function to execute data ingestion.

    Args:
        args (list): Lista de argumentos passados para a função.
                     List of arguments passed to the function.

    Returns:
        None
    """
    try:
        log_info('Ingestão iniciada')
        # Data ingestion started

        # Processa os argumentos
        # Process the arguments
        print(args)
        root_properties = process_args(args)

        # Imprime as propriedades raiz
        # Print the root properties
        for p in root_properties:
            log_info(f'{p}: {root_properties[p]}')

        # Criação de SparkSession
        # Create SparkSession
        spark = SparkSessionManager.create_spark_session('Carlton Ingest APP')

        # Leitura de dados
        # Read data

        df = read(spark, root_properties)

        # Salvamento de dados
        # Save data
        # save(df, root_properties)

        log_info('Ingestão finalizada')
        # Data ingestion finished

    except Exception as e:
        # Loga qualquer erro que ocorrer
        # Log any error that occurs
        log_error(str(e))


if __name__ == '__main__':
    main()
