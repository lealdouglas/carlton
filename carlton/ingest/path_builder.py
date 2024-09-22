# path_builder.py
class PathBuilder:
    @staticmethod
    def build_adls_paths(config):
        """
        Build paths for Azure Data Lake Storage (ADLS).

        :param config: Configuration dictionary containing necessary keys.
        :return: Updated configuration dictionary with ADLS paths.
        """
        config[
            'carlton_file_path'
        ] = f"abfss://{config['container_src']}@{config['storage_name_src']}.dfs.windows.net/{config['path_src']}/"
        config[
            'schemaLocation'
        ] = f"abfss://{config['container_src']}@{config['storage_name_src']}.dfs.windows.net/{config['table_name']}/_schemaLocation"
        config[
            'table_path'
        ] = f"abfss://{config['container_tgt']}@{config['storage_name_tgt']}.dfs.windows.net/{config['table_name']}/"
        config[
            'checkpointLocation'
        ] = f"abfss://{config['container_tgt']}@{config['storage_name_tgt']}.dfs.windows.net/{config['table_name']}/_checkpointLocation"
        return config
