# table_manager.py
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from carlton.utils.logger import log_error, log_info


class TableManager:
    @staticmethod
    def drop_table_path(schema_name: str, table_name: str):
        """
        Remove a tabela especificada e seu caminho associado.\n
        Drops the specified table and its associated path.

        Args:
            schema_name (str): O nome do esquema da tabela.
                               The schema name of the table.
            table_name (str): O nome da tabela a ser removida.
                              The table name to be dropped.
        """
        try:
            # Obtém ou cria uma sessão Spark
            # Get or create a Spark session
            spark = SparkSessionManager.get_spark_session()

            # Determina o dbutils apropriado com base no ambiente
            # Determine the appropriate dbutils based on the environment
            if (
                spark.conf.get('spark.databricks.service.client.enabled')
                == 'true'
            ):
                from pyspark.dbutils import DBUtils

                dbutils = DBUtils(spark)
            else:
                import IPython

                dbutils = IPython.get_ipython().user_ns['dbutils']

            # Constrói o nome da tabela e obtém sua localização
            # Construct the table name and get its location
            table = f'{schema_name}.{table_name}'
            location = (
                DeltaTable.forName(spark, table)
                .detail()
                .select('location')
                .first()[0]
            )

            log_info(f'location da tabela que sera apagada: {location}')
            # Location of the table to be dropped: {location}

            # Remove a localização da tabela e exclui a tabela
            # Remove the table's location and drop the table
            dbutils.fs.rm(location, True)
            spark.sql(f'DROP TABLE IF EXISTS {table}')

            log_info(f'{table} removida com sucesso')
            # {table} successfully removed

        except Exception as e:
            # Registra qualquer erro que ocorra durante a operação de exclusão
            # Log any error that occurs during the drop operation
            log_error(str(e))
