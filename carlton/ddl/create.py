from datacontract.data_contract import DataContract
from pyspark.sql import DataFrame, SparkSession


def get_absolute_path(*relative_parts):
    import os

    if 'dbutils' in globals():
        # dbutils is available to the entry point when run in Databricks
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
        return os.path.join(*relative_parts)


def create(spark: SparkSession):
    data_contract_file = get_absolute_path(
        '/jarvis/prep/datacontract/', 'datacontract.yaml'
    )

    data_contract = DataContract(data_contract_file=data_contract_file)
    sql_ddl = data_contract.export(export_format='sql')

    print(data_contract_file)
    print(sql_ddl)

    # Create the table in Unity Catalog
    # spark.sql(sql_ddl)
