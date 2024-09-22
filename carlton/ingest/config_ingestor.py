# config_ingestor.py
from carlton.ingest.config_validator import ConfigValidator
from carlton.ingest.path_builder import PathBuilder
from carlton.utils.logger import log_error, log_info


class ConfigIngestor:
    @staticmethod
    def get_params_path(config):
        """
        Obtém os caminhos apropriados com base no tipo de recurso do arquivo.\n
        Get the appropriate paths based on the file resource type.

        Args:
            config (dict): Dicionário de configuração contendo as chaves necessárias.
                           Configuration dictionary containing necessary keys.

        Returns:
            dict: Dicionário de configuração atualizado com os caminhos.
                  Updated configuration dictionary with paths.

        Example:
            >>> config = {'file_resource': 'adls','container_src': 'source_container','storage_name_src': 'source_storage', 'container_tgt': 'target_container','storage_name_tgt': 'target_storage','path_src': 'source_path','table_name': 'name_table'}
            >>> updated_config = ConfigIngestor.get_params_path(config)
        """
        if config['file_resource'] == 'adls':
            ConfigValidator.validate_args(
                [
                    'container_src',
                    'storage_name_src',
                    'container_tgt',
                    'storage_name_tgt',
                    'path_src',
                    'table_name',
                ],
                config,
            )
            log_info('Building paths for ADLS')
            config = PathBuilder.build_adls_paths(config)
        return config

    @staticmethod
    def config_ingest_src(config, custom_config_spark={}):
        """
        Configura as configurações de ingestão da fonte.\n
        Configure the source ingestion settings.

        Args:
            config (dict): Dicionário de configuração contendo as chaves necessárias.
                           Configuration dictionary containing necessary keys.
            custom_config_spark (dict, optional): Dicionário de configuração personalizada do Spark.
                                                  Custom Spark configuration dictionary.

        Returns:
            dict: Dicionário com a configuração do autoloader.
                  Dictionary with autoloader configuration.

        Example:
            >>> config = {'type_run': 'batch','file_extension': 'csv','file_resource': 'adls','container_src': 'source_container','storage_name_src': 'source_storage','container_tgt': 'target_container','storage_name_tgt': 'target_storage','path_src': 'source_path','file_header': 'true','file_delimiter': ',','schemaLocation': 'schema_location','table_name': 'name_table'}
            >>> autoloader_config = ConfigIngestor.config_ingest_src(config)
        """
        ConfigValidator.validate_args(
            ['type_run', 'file_extension', 'file_resource'], config
        )

        log_info('prepara os caminhos apropriados com base no tipo de recurso do arquivo')
        config = ConfigIngestor.get_params_path(config)

        autoloader_config_csv = {}
        if config['file_extension'] == 'csv':
            ConfigValidator.validate_args(
                ['file_header', 'file_delimiter'], config
            )
            autoloader_config_csv = {
                'header': config['file_header'],
                'delimiter': config['file_delimiter'],
            }

        autoloader_config_json = {}
        if config['file_extension'] == 'json':
            autoloader_config_json = {
                'multiLine': True,
            }

        autoloader_config = {
            'pathGlobfilter': f"*.{config['file_extension']}",
            'cloudFiles.format': config['file_extension'],
            'cloudFiles.schemaLocation': config['schemaLocation'],
            'cloudFiles.schemaEvolutionMode': 'rescue',
            'cloudFiles.inferColumnTypes': 'false',
            'cloudFiles.allowOverwrites': 'true',
            'rescuedDataColumn': 'carlton_rescued',
            **autoloader_config_csv,
            **autoloader_config_json,
            **custom_config_spark,
        }

        return autoloader_config

    @staticmethod
    def config_ingest_tgt(config, custom_config_spark={}):
        """
        Configura as configurações de ingestão do destino.\n
        Configure the target ingestion settings.

        Args:
            config (dict): Dicionário de configuração contendo as chaves necessárias.
                           Configuration dictionary containing necessary keys.
            custom_config_spark (dict, optional): Dicionário de configuração personalizada do Spark.
                                                  Custom Spark configuration dictionary.

        Returns:
            dict: Dicionário com a configuração de salvamento.
                  Dictionary with save configuration.

        Example:
            >>> config = {'table_path': '/path/to/table','checkpointLocation': '/path/to/checkpoint'}
            >>> save_config = ConfigIngestor.config_ingest_tgt(config)
        """
        ConfigValidator.validate_args(
            ['table_path', 'checkpointLocation'], config
        )

        save_config = {
            'checkpointLocation': config['checkpointLocation'],
            'path': config['table_path'],
            'mergeSchema': True,
            **custom_config_spark,
        }

        return save_config
