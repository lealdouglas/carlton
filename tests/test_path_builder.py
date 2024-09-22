import pytest

from carlton.ingest.path_builder import PathBuilder


@pytest.fixture
def config():
    return {
        'container_src': 'source_container',
        'storage_name_src': 'source_storage',
        'container_tgt': 'target_container',
        'storage_name_tgt': 'target_storage',
        'path_src': 'source_path',
        'table_name': 'table_name',
    }


def test_build_adls_paths(config):
    """
    Testa a função build_adls_paths para verificar se os caminhos ADLS são construídos corretamente.
    Tests the build_adls_paths function to check if ADLS paths are built correctly.
    """
    expected_config = {
        'container_src': 'source_container',
        'storage_name_src': 'source_storage',
        'container_tgt': 'target_container',
        'storage_name_tgt': 'target_storage',
        'path_src': 'source_path',
        'table_name': 'table_name',
        'carlton_file_path': 'abfss://source_container@source_storage.dfs.windows.net/source_path/',
        'schemaLocation': 'abfss://source_container@source_storage.dfs.windows.net/table_name/_schemaLocation',
        'table_path': 'abfss://target_container@target_storage.dfs.windows.net/table_name/',
        'checkpointLocation': 'abfss://target_container@target_storage.dfs.windows.net/table_name/_checkpointLocation',
    }

    result = PathBuilder.build_adls_paths(config)
    assert result == expected_config
