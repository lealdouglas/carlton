# config_validator.py
from carlton.utils.helper import validate_args

class ConfigValidator:
    @staticmethod
    def validate_args(required_args, config):
        """
        Validate that all required arguments are present in the config.
        
        :param required_args: List of required argument keys.
        :param config: Configuration dictionary to validate.
        """
        validate_args(required_args, config)

# path_builder.py
class PathBuilder:
    @staticmethod
    def build_adls_paths(config):
        """
        Build paths for Azure Data Lake Storage (ADLS).
        
        :param config: Configuration dictionary containing necessary keys.
        :return: Updated configuration dictionary with ADLS paths.
        """
        config['carlton_file_path'] = f"abfss://{config['container_src']}@{config['storage_name_src']}.dfs.windows.net/{config['path_src']}/"
        config['schemaLocation'] = f"abfss://{config['container_src']}@{config['storage_name_src']}.dfs.windows.net/{config['table_name']}/_schemaLocation"
        config['table_path'] = f"abfss://{config['container_tgt']}@{config['storage_name_tgt']}.dfs.windows.net/{config['table_name']}/"
        config['checkpointLocation'] = f"abfss://{config['container_tgt']}@{config['storage_name_tgt']}.dfs.windows.net/{config['table_name']}/_checkpointLocation"
        return config

    @staticmethod
    def build_dbfs_paths(config):
        """
        Build paths for Databricks File System (DBFS).
        
        :param config: Configuration dictionary containing necessary keys.
        :return: Updated configuration dictionary with DBFS paths.
        """
        config['carlton_file_path'] = f"{config['file_dbfs']}/input/{config['path_src']}/"
        config['schemaLocation'] = f"{config['file_dbfs']}/input/{config['table_name']}/_schemaLocation"
        config['table_path'] = f"{config['file_dbfs']}/output/{config['table_name']}"
        config['checkpointLocation'] = f"{config['file_dbfs']}/output/{config['table_name']}/_checkpointLocation"
        return config

# config_ingestor.py
from carlton.utils.logger import log_error, log_info
from config_validator import ConfigValidator
from path_builder import PathBuilder

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
                ['container_src', 'storage_name_src', 'container_tgt', 'storage_name_tgt', 'path_src'],
                config
            )
            config = PathBuilder.build_adls_paths(config)
        elif config['file_resource'] == 'dbfs':
            ConfigValidator.validate_args(
                ['file_dbfs', 'table_name'],
                config
            )
            config = PathBuilder.build_dbfs_paths(config)
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
            ['type_run', 'file_extension', 'file_resource'],
            config
        )
        config = ConfigIngestor.get_params_path(config)

        autoloader_config_csv = {}
        if config['file_extension'] == 'csv':
            ConfigValidator.validate_args(
                ['file_header', 'file_delimiter'],
                config
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
            ['table_path', 'checkpointLocation'],
            config
        )

        save_config = {
            'checkpointLocation': config['checkpointLocation'],
            'path': config['table_path'],
            'mergeSchema': True,
            **custom_config_spark,
        }

        return save_config