from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession

from carlton.ingest.table import read, save


@patch('carlton.ingest.table.DataReader')
@patch('carlton.ingest.table.log_info')
@patch('carlton.ingest.table.log_error')
def test_read(mock_log_error, mock_log_info, mock_data_reader):
    """
    Testa a função read para verificar se os dados são lidos corretamente.
    """
    # Mock the Spark session and DataFrame
    mock_spark = MagicMock(spec=SparkSession)
    mock_df = MagicMock(spec=DataFrame)
    mock_data_reader.read_data.return_value = mock_df

    # Configurações de ingestão
    config_ingest = {'schema_name': 'test_schema', 'table_name': 'test_table'}
    custom_config_spark = {'spark.executor.memory': '2g'}

    # Call the read function
    result_df = read(mock_spark, config_ingest, custom_config_spark)

    # Assertions
    mock_log_info.assert_called_once_with('DataReader.read_data')
    mock_data_reader.read_data.assert_called_once_with(
        mock_spark, config_ingest, custom_config_spark
    )
    assert result_df == mock_df
    mock_log_error.assert_not_called()


@patch('carlton.ingest.table.DataSaver')
@patch('carlton.ingest.table.log_info')
@patch('carlton.ingest.table.log_error')
def test_save(mock_log_error, mock_log_info, mock_data_saver):
    """
    Testa a função save para verificar se os dados são salvos corretamente.
    """
    # Mock the Spark session and DataFrame
    mock_spark = MagicMock(spec=SparkSession)
    mock_df = MagicMock(spec=DataFrame)

    # Configurações de ingestão
    config_ingest = {'schema_name': 'test_schema', 'table_name': 'test_table'}
    custom_config_spark = {'spark.executor.memory': '2g'}

    # Call the save function
    save(mock_spark, mock_df, config_ingest, custom_config_spark)

    # Assertions
    mock_log_info.assert_called_once_with('DataSaver.save_data')
    mock_data_saver.save_data.assert_called_once_with(
        mock_spark, mock_df, config_ingest, custom_config_spark
    )
    mock_log_error.assert_not_called()


@patch('carlton.ingest.table.DataReader')
@patch('carlton.ingest.table.log_info')
@patch('carlton.ingest.table.log_error')
def test_read_exception(mock_log_error, mock_log_info, mock_data_reader):
    """
    Testa a função read para verificar se as exceções são tratadas corretamente.
    """
    # Mock the Spark session
    mock_spark = MagicMock(spec=SparkSession)
    mock_data_reader.read_data.side_effect = Exception('Test exception')

    # Configurações de ingestão
    config_ingest = {'schema_name': 'test_schema', 'table_name': 'test_table'}
    custom_config_spark = {'spark.executor.memory': '2g'}

    # Call the read function and expect an exception
    with pytest.raises(Exception, match='Test exception'):
        read(mock_spark, config_ingest, custom_config_spark)

    # Assertions
    mock_log_info.assert_called_once_with('DataReader.read_data')
    mock_data_reader.read_data.assert_called_once_with(
        mock_spark, config_ingest, custom_config_spark
    )
    mock_log_error.assert_called_once_with(
        'Erro na função read: Test exception'
    )


@patch('carlton.ingest.table.DataSaver')
@patch('carlton.ingest.table.log_info')
@patch('carlton.ingest.table.log_error')
def test_save_exception(mock_log_error, mock_log_info, mock_data_saver):
    """
    Testa a função save para verificar se as exceções são tratadas corretamente.
    """
    # Mock the Spark session and DataFrame
    mock_spark = MagicMock(spec=SparkSession)
    mock_df = MagicMock(spec=DataFrame)
    mock_data_saver.save_data.side_effect = Exception('Test exception')

    # Configurações de ingestão
    config_ingest = {'schema_name': 'test_schema', 'table_name': 'test_table'}
    custom_config_spark = {'spark.executor.memory': '2g'}

    # Call the save function and expect an exception
    with pytest.raises(Exception, match='Test exception'):
        save(mock_spark, mock_df, config_ingest, custom_config_spark)

    # Assertions
    mock_log_info.assert_called_once_with('DataSaver.save_data')
    mock_data_saver.save_data.assert_called_once_with(
        mock_spark, mock_df, config_ingest, custom_config_spark
    )
    mock_log_error.assert_called_once_with(
        'Erro na função save: Test exception'
    )
