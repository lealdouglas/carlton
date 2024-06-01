from datetime import datetime, timezone

import pytz
from delta.tables import *

SP = pytz.timezone('America/Sao_Paulo')


def validate_args(args_nedeed: list, args_user: dict):
    """
    Valida se o dicionario passado na funcao consta com os parametros obrigatorios da funcao.

    Args:
        args_nedeed: Lista de configuracoes obrigatorias que a funcao utiliza
        args_user: dicionario de configuracoes do usuario

    Raises:
        KeyError: Caso nao identifique um parametro obrigatorio no dicionario do usuario.

    Examples:
        >>> validate_args(['table_checkpoint_location','table_path'],{'table_checkpoint_location':'/save/_checkpointLocation','table_path':'/save/'})
    """

    for arg in args_nedeed:
        if arg not in args_user:
            raise KeyError(
                f'Nao foi possivel localizar o parametro: {arg} . Por favor adicionar'
            )


def carlton_log(msg=str, msg_dict={}, msg_lst=[]):

    utc_dt = datetime.now(timezone.utc)
    print(
        f"CARLTON_LOG {utc_dt.astimezone(SP).isoformat()}: {msg.upper()} {'' if len(msg_dict)==0 else msg_dict} {'' if len(msg_dict)==0 else msg_lst}"
    )


def drop_table_path(schema_name: str, table_name: str):

    try:

        spark = SparkSession.builder.getOrCreate()

        if spark.conf.get('spark.databricks.service.client.enabled') == 'true':
            from pyspark.dbutils import DBUtils

            dbutils = DBUtils(spark)
        else:
            import IPython

            dbutils = IPython.get_ipython().user_ns['dbutils']

        table = f'{schema_name}.{table_name}'
        location = (
            DeltaTable.forName(spark, table)
            .detail()
            .select('location')
            .first()[0]
        )

        carlton_log(f'location da tabela que sera apagada: {location}')

        dbutils.fs.rm(location, True)
        spark.sql(f'DROP TABLE IF EXISTS {table}')

        carlton_log(f'{table} removida com sucesso')

    except Exception as e:
        carlton_log(e)
