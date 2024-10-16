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
        ] = f"abfss://{config['container_src']}@{config['storage_name_src']}.dfs.core.windows.net/{config['path_src']}/"
        config[
            'schemaLocation'
        ] = f"abfss://{config['container_src']}@{config['storage_name_src']}.dfs.core.windows.net/_schemaLocation/{config['table_name']}/"
        config[
            'table_path'
        ] = f"abfss://{config['container_tgt']}@{config['storage_name_tgt']}.dfs.core.windows.net/{config['table_name']}/"
        config[
            'checkpointLocation'
        ] = f"/Volumes/crisk/bronze/volume_checkpoint_locations/{config['table_name']}/"
        return config


# ] = f"abfss://{config['container_tgt']}@{config['storage_name_tgt']}.dfs.core.windows.net/{config['table_name']}/_checkpointLocation"
