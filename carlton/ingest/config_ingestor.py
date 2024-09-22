# config_ingestor.py
from config_validator import ConfigValidator
from path_builder import PathBuilder

from carlton.utils.logger import log_error, log_info


class ConfigIngestor:
    @staticmethod
    def get_params_path(config):
        """
        Get the appropriate paths based on the file resource type.

        :param config: Configuration dictionary containing necessary keys.
        :return: Updated configuration dictionary with paths.
        """
        if config['file_resource'] == 'adls':
            ConfigValidator.validate_args(
                [
                    'container_src',
                    'storage_name_src',
                    'container_tgt',
                    'storage_name_tgt',
                    'path_src',
                ],
                config,
            )
            config = PathBuilder.build_adls_paths(config)
        return config

    @staticmethod
    def config_ingest_src(config, custom_config_spark={}):
        """
        Configure the source ingestion settings.

        :param config: Configuration dictionary containing necessary keys.
        :param custom_config_spark: Custom Spark configuration dictionary.
        :return: Dictionary with autoloader configuration.
        """
        ConfigValidator.validate_args(
            ['type_run', 'file_extension', 'file_resource'], config
        )
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
        Configure the target ingestion settings.

        :param config: Configuration dictionary containing necessary keys.
        :param custom_config_spark: Custom Spark configuration dictionary.
        :return: Dictionary with save configuration.
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
