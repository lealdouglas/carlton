from carlton.helper import carlton_log, validate_args


def get_params_path(config_ingest: dict) -> dict:

    if config_ingest['file_resource'] == 'gen2':

        validate_args(
            [
                'container_src',
                'storage_name_src',
                'container_tgt',
                'storage_name_tgt',
            ],
            config_ingest,
        )

        config_ingest[
            'carlton_file_path'
        ] = f"abfss://{config_ingest['container_src']}@{config_ingest['storage_name_src']}.dfs.windows.net/{config_ingest['table_name']}/"
        config_ingest[
            'schemaLocation'
        ] = f"abfss://{config_ingest['container_src']}@{config_ingest['storage_name_src']}.dfs.windows.net/{config_ingest['table_name']}/_schemaLocation"
        config_ingest[
            'table_path'
        ] = f"abfss://{config_ingest['container_tgt']}@{config_ingest['storage_name_tgt']}.dfs.windows.net/{config_ingest['table_name']}/"
        config_ingest[
            'checkpointLocation'
        ] = f"abfss://{config_ingest['container_tgt']}@{config_ingest['storage_name_tgt']}.dfs.windows.net/{config_ingest['table_name']}/_checkpointLocation"

    elif config_ingest['file_resource'] == 'dbfs':

        validate_args(
            [
                'file_dbfs',
                'table_name',
            ],
            config_ingest,
        )

        config_ingest[
            'carlton_file_path'
        ] = f"{config_ingest['file_dbfs']}/input/{config_ingest['table_name']}/"
        config_ingest[
            'schemaLocation'
        ] = f"{config_ingest['file_dbfs']}/input/{config_ingest['table_name']}/_schemaLocation"
        config_ingest[
            'table_path'
        ] = f"{config_ingest['file_dbfs']}/output/{config_ingest['table_name']}"
        config_ingest[
            'checkpointLocation'
        ] = f"{config_ingest['file_dbfs']}/output/{config_ingest['table_name']}/_checkpointLocation"

    return config_ingest


def config_ingest_src(config_ingest: dict, custom_config_spark={}) -> dict:
    """
    Prepara as variaveis de configuracao para leitura do arquivo.

    Args:
        custom_config_spark: dicionario de configuracoes adicionais se necessario

    Returns:
        Um dicionario de configuracoes.

    Examples:
        >>> config_ingest_src({'file_resource':'dbfs','file_dbfs':'/FileStore/dt_master','table_name':'account_json','type_run':'batch','file_extension':'csv','file_header':'true','file_delimiter':';','table_checkpoint_location':'/save/_checkpointLocation','table_path':'/save/','table_merge_schema':'true'})
        {'pathGlobfilter': '*.csv', 'cloudFiles.format': 'csv', 'cloudFiles.schemaLocation': '/FileStore/dt_master/input/account_json/_schemaLocation', 'cloudFiles.schemaEvolutionMode': 'rescue', 'cloudFiles.inferColumnTypes': 'false', 'cloudFiles.allowOverwrites': 'true', 'rescuedDataColumn': 'carlton_rescued', 'header': 'true', 'delimiter': ';'}
    """

    validate_args(
        [
            'type_run',
            'file_extension',
            'file_resource',
        ],
        config_ingest,
    )

    config_ingest = get_params_path(config_ingest)

    autoloader_config_csv = {}
    if config_ingest['file_extension'] == 'csv':

        validate_args(
            [
                'file_header',
                'file_delimiter',
            ],
            config_ingest,
        )

        autoloader_config_csv = {
            'header': config_ingest['file_header'],
            'delimiter': config_ingest['file_delimiter'],
        }

    autoloader_config_json = {}
    if config_ingest['file_extension'] == 'json':
        autoloader_config_json = {
            'multiLine': True,
            # "encoding": "utf8",
        }

    autoloader_config = {
        'pathGlobfilter': f"*.{config_ingest['file_extension']}",
        'cloudFiles.format': config_ingest['file_extension'],
        'cloudFiles.schemaLocation': config_ingest['schemaLocation'],
        'cloudFiles.schemaEvolutionMode': 'rescue',
        'cloudFiles.inferColumnTypes': 'false',
        'cloudFiles.allowOverwrites': 'true',
        'rescuedDataColumn': 'carlton_rescued',
        **autoloader_config_csv,
        **autoloader_config_json,
        **custom_config_spark,
    }

    return autoloader_config


def config_ingest_tgt(config_ingest: dict, custom_config_spark={}) -> dict:
    """
    Prepara as variaveis de configuracao para leitura do arquivo.

    Args:
        custom_config_spark: dicionario de configuracoes adicionais se necessario

    Returns:
        Um dicionario de configuracoes.

    Examples:
        >>> config_ingest_tgt({'checkpointLocation':'/save/_checkpointLocation','table_path':'/save','table_merge_schema':'true','type_run':'batch','file_extension':'csv','file_header':'true','file_delimiter':';','schema_location':'/input/'})
        {'checkpointLocation': '/save/_checkpointLocation', 'path': '/save', 'mergeSchema': True}
    """

    args_nedeed = [
        'table_path',
        'checkpointLocation',
    ]

    validate_args(args_nedeed, config_ingest)

    save_config = {
        'checkpointLocation': config_ingest['checkpointLocation'],
        'path': config_ingest['table_path'],
        'mergeSchema': True,
        **custom_config_spark,
    }

    return save_config
