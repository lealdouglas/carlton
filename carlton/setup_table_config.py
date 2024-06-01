from pyspark.sql import SparkSession


def setup_table(schema_setup_name='bronze'):

    try:
        spark = SparkSession.builder.getOrCreate()

        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {schema_setup_name}.table_config_ingest (
            SCHEMA STRING,
            TABLE STRING,
            DOMAIN STRING,
            REQUESTER STRING
            )
            USING DELTA CLUSTER BY (DOMAIN, SCHEMA)
        """
        )

        carlton_log(
            f'{schema_setup_name}.table_config_ingest criado com sucesso'
        )
    except Exception as e:
        carlton_log(e)


def insert_value_table(
    table_name: str,
    domain_name: str,
    requester_name: str,
    schema_name='bronze',
    schema_setup_name='bronze',
):

    try:

        spark = SparkSession.builder.getOrCreate()

        result_query = spark.sql(
            f"SELECT count(1) FROM {schema_setup_name}.table_config_ingest WHERE SCHEMA='{schema_name}' AND TABLE='{table_name}' AND DOMAIN='{domain_name}'"
        ).first()[0]

        if result_query == 1:
            raise Exception(f'{schema_name}.{table_name} ja em uso')

        spark.sql(
            f"INSERT INTO TABLE {schema_setup_name}.table_config_ingest VALUES ('{schema_name}','{table_name}','{domain_name}','{requester_name}')"
        )

        carlton_log(f'{schema_name}.{table_name} cadastrada com sucesso')
    except Exception as e:
        carlton_log(e)


def get_tables_domain(domain_name: str, schema_setup_name='bronze'):

    try:

        spark = SparkSession.builder.getOrCreate()

        display(
            spark.sql(
                f"SELECT * FROM {schema_setup_name}.table_config_ingest WHERE DOMAIN='{domain_name}'"
            )
        )
    except Exception as e:
        carlton_log(e)


def get_tables_requester(requester: str, schema_setup_name='bronze'):
    try:

        spark = SparkSession.builder.getOrCreate()

        display(
            spark.sql(
                f"SELECT * FROM {schema_setup_name}.table_config_ingest WHERE REQUESTER='{requester}'"
            )
        )
    except Exception as e:
        carlton_log(e)
