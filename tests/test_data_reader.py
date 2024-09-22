from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from carlton.ingest.data_reader import DataReader


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


# @patch('carlton.ingest.data_reader.ConfigIngestor.config_ingest_src')
# @patch('carlton.ingest.data_reader.SparkSession.builder.getOrCreate')
# @patch('carlton.ingest.data_reader.log_info')
# @patch('carlton.ingest.data_reader.log_error')
# def test_read_data(
#     mock_log_error,
#     mock_log_info,
#     mock_getOrCreate,
#     mock_config_ingest_src,
#     config_ingest,
#     custom_config_spark,
# ):
#     """
#     Testa a função read_data para verificar se os dados são lidos corretamente.
#     Tests the read_data function to check if data is read correctly.
#     """
#     # Mock the Spark session and DataFrame
#     mock_spark = mock_getOrCreate.return_value
#     mock_df = MagicMock()
#     mock_spark.readStream.format.return_value.options.return_value.load.return_value.select.return_value = (
#         mock_df
#     )

#     # Mock the config_ingest_src function
#     mock_config_ingest_src.return_value = {
#         'cloudFiles.format': 'csv',
#         'header': 'true',
#         'delimiter': ',',
#         'schemaLocation': 'schema_location',
#     }

#     # Call the read_data function
#     result = DataReader.read_data(config_ingest, custom_config_spark)

#     # Assertions
#     mock_config_ingest_src.assert_called_once_with(
#         config_ingest, custom_config_spark
#     )
#     mock_log_info.assert_any_call(
#         'Configurations used for reading: ',
#         msg_dict=mock_config_ingest_src.return_value,
#     )
#     mock_getOrCreate.assert_called_once()
#     mock_spark.readStream.format.assert_called_once_with('cloudFiles')
#     mock_spark.readStream.format.return_value.options.assert_called_once_with(
#         **mock_config_ingest_src.return_value
#     )
#     mock_spark.readStream.format.return_value.options.return_value.load.assert_called_once_with(
#         'path/to/data'
#     )
#     mock_spark.readStream.format.return_value.options.return_value.load.return_value.select.assert_called_once_with(
#         '*',
#         mock_spark.sql.functions.current_date().alias('carlton_current_date'),
#         mock_spark.sql.functions.col('_metadata').alias('carlton_metadata'),
#     )
#     assert result == mock_df

#     # Test exception handling
#     mock_spark.readStream.format.return_value.options.return_value.load.side_effect = Exception(
#         'Test exception'
#     )
#     result = DataReader.read_data(config_ingest, custom_config_spark)
#     mock_log_error.assert_called_once_with('Test exception')
#     assert result is None
