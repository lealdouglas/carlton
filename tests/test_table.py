from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame

from carlton.ingest.table import read, save


@pytest.fixture
def config_ingest():
    return {
        'type_run': 'batch',
        'file_extension': 'csv',
        'file_resource': 'adls',
        'container_src': 'source_container',
        'storage_name_src': 'source_storage',
        'container_tgt': 'target_container',
        'storage_name_tgt': 'target_storage',
        'path_src': 'source_path',
        'file_header': 'true',
        'file_delimiter': ',',
        'schemaLocation': 'schema_location',
        'carlton_file_path': 'path/to/data',
    }


@pytest.fixture
def custom_config_spark():
    return {'spark.some.config.option': 'some-value'}


@patch('carlton.ingest.table.DataReader.read_data')
def test_read(mock_read_data, config_ingest, custom_config_spark):
    """
    Testa a função read para verificar se os dados são lidos corretamente.
    Tests the read function to check if data is read correctly.
    """
    # Mock the DataFrame
    mock_df = MagicMock(spec=DataFrame)
    mock_read_data.return_value = mock_df

    # Call the read function
    result = read(config_ingest, custom_config_spark)

    # Assertions
    mock_read_data.assert_called_once_with(config_ingest, custom_config_spark)
    assert result == mock_df


@patch('carlton.ingest.table.DataSaver.save_data')
def test_save(mock_save_data, config_ingest, custom_config_spark):
    """
    Testa a função save para verificar se os dados são salvos corretamente.
    Tests the save function to check if data is saved correctly.
    """
    # Mock the DataFrame
    mock_df = MagicMock(spec=DataFrame)

    # Call the save function
    save(mock_df, config_ingest, custom_config_spark)

    # Assertions
    mock_save_data.assert_called_once_with(
        mock_df, config_ingest, custom_config_spark
    )
