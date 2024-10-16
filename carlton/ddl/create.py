from datacontract.data_contract import DataContract
from pyspark.sql import DataFrame, SparkSession


def get_absolute_path(*relative_parts):
    """
    Retorna o caminho absoluto baseado nas partes relativas fornecidas.
    Se estiver sendo executado no Databricks, utiliza dbutils para obter o caminho do notebook.
    Caso contrário, utiliza os caminhos relativos fornecidos.

    Args:
        *relative_parts: Partes do caminho relativo.

    Returns:
        str: Caminho absoluto.
    """
    import os

    if 'dbutils' in globals():
        # dbutils está disponível no ponto de entrada quando executado no Databricks
        base_dir = os.path.dirname(
            dbutils.notebook.entry_point.getDbutils()
            .notebook()
            .getContext()
            .notebookPath()
            .get()
        )   # type: ignore
        path = os.path.normpath(os.path.join(base_dir, *relative_parts))
        return path if path.startswith('/Workspace') else '/Workspace' + path
    else:
        # Retorna o caminho absoluto baseado nas partes relativas fornecidas
        return os.path.join(*relative_parts)


def create(spark: SparkSession, src='adb'):
    """
    Cria uma tabela no Unity Catalog usando um contrato de dados.

    Args:
        spark (SparkSession): Sessão Spark.
        src (str): Fonte dos dados, padrão é 'adb'.

    Returns:
        None
    """
    # Obtém o caminho absoluto do arquivo de contrato de dados
    data_contract_file = get_absolute_path(
        '/Workspace/jarvis/prep/datacontract/', 'datacontract.yaml'
    )

    # Cria uma instância do DataContract usando o arquivo de contrato de dados
    data_contract = DataContract(data_contract_file=data_contract_file)

    # Exporta o contrato de dados no formato SQL DDL
    sql_ddl = data_contract.export(export_format='sql')

    # Imprime o caminho do arquivo de contrato de dados e o SQL DDL gerado
    print(data_contract_file)
    print(sql_ddl)

    # Cria a tabela no Unity Catalog usando o SQL DDL gerado
    spark.sql(sql_ddl)
