from unittest.mock import MagicMock, patch

import pytest

from carlton.ingest.config_ingestor import ConfigIngestor


@pytest.fixture
def config_adls():
    return {
        'file_resource': 'adls',
        'container_src': 'source_container',
        'storage_name_src': 'source_storage',
        'container_tgt': 'target_container',
        'storage_name_tgt': 'target_storage',
        'path_src': 'source_path',
        'table_name': 'table_name',
        'file_extension': 'csv',
        'file_header': 'true',
        'file_delimiter': ',',
        'schemaLocation': 'schema_location',
    }


@pytest.fixture
def config_dbfs():
    return {
        'file_resource': 'dbfs',
        'file_dbfs': '/dbfs/path',
        'table_name': 'table_name',
        'path_src': 'source_path',
    }


@pytest.fixture
def config_tgt():
    return {
        'table_path': '/path/to/table',
        'checkpointLocation': '/path/to/checkpoint',
    }


@patch('carlton.ingest.config_validator.ConfigValidator.validate_args')
@patch('carlton.ingest.config_ingestor.PathBuilder.build_adls_paths')
def test_get_params_path_adls(
    mock_build_adls_paths, mock_validate_args, config_adls
):
    """
    Testa a função get_params_path para o recurso de arquivo ADLS.
    Tests the get_params_path function for ADLS file resource.
    """
    mock_build_adls_paths.return_value = config_adls
    result = ConfigIngestor.get_params_path(config_adls)
    mock_validate_args.assert_called_once_with(
        [
            'container_src',
            'storage_name_src',
            'container_tgt',
            'storage_name_tgt',
            'path_src',
            'table_name',
        ],
        config_adls,
    )
    mock_build_adls_paths.assert_called_once_with(config_adls)
    assert result == config_adls


@patch('carlton.ingest.config_ingestor.ConfigValidator.validate_args')
@patch('carlton.ingest.config_ingestor.ConfigIngestor.get_params_path')
def test_config_ingest_src_csv(
    mock_get_params_path, mock_validate_args, config_adls
):
    """
    Testa a função config_ingest_src para arquivos CSV.
    Tests the config_ingest_src function for CSV files.
    """
    mock_get_params_path.return_value = config_adls
    result = ConfigIngestor.config_ingest_src(config_adls)
    mock_validate_args.assert_any_call(
        ['type_run', 'file_extension', 'file_resource'], config_adls
    )
    mock_validate_args.assert_any_call(
        ['file_header', 'file_delimiter'], config_adls
    )
    mock_get_params_path.assert_called_once_with(config_adls)
    assert result['cloudFiles.format'] == 'csv'
    assert result['header'] == 'true'
    assert result['delimiter'] == ','


@patch('carlton.ingest.config_ingestor.ConfigValidator.validate_args')
@patch('carlton.ingest.config_ingestor.ConfigIngestor.get_params_path')
def test_config_ingest_src_json(
    mock_get_params_path, mock_validate_args, config_adls
):
    """
    Testa a função config_ingest_src para arquivos JSON.
    Tests the config_ingest_src function for JSON files.
    """
    config_adls['file_extension'] = 'json'
    mock_get_params_path.return_value = config_adls
    result = ConfigIngestor.config_ingest_src(config_adls)
    mock_validate_args.assert_any_call(
        ['type_run', 'file_extension', 'file_resource'], config_adls
    )
    mock_get_params_path.assert_called_once_with(config_adls)
    assert result['cloudFiles.format'] == 'json'
    assert result['multiLine'] == True


@patch('carlton.ingest.config_ingestor.ConfigValidator.validate_args')
def test_config_ingest_tgt(mock_validate_args, config_tgt):
    """
    Testa a função config_ingest_tgt.
    Tests the config_ingest_tgt function.
    """
    result = ConfigIngestor.config_ingest_tgt(config_tgt)
    mock_validate_args.assert_called_once_with(
        ['table_path', 'checkpointLocation'], config_tgt
    )
    assert result['checkpointLocation'] == '/path/to/checkpoint'
    # assert result['path'] == '/path/to/table'
    assert result['mergeSchema'] == True
