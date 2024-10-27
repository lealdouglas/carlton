from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from carlton.ingest.data_saver import DataSaver


@pytest.fixture
def spark():
    return MagicMock(spec=SparkSession)


@pytest.fixture
def df():
    return MagicMock()


@pytest.fixture
def config_ingest():
    return {
        'schema_name': 'test_schema',
        'table_name': 'test_table',
        'table_path': '/path/to/table',
        'type_run': 'batch',
    }


@pytest.fixture
def custom_config_spark():
    return {'spark.some.config.option': 'some-value'}


# @patch('carlton.ingest.data_saver.ConfigValidator.validate_args')
# @patch('carlton.ingest.data_saver.ConfigIngestor.config_ingest_tgt')
# @patch('carlton.ingest.data_saver.log_info')
# @patch('carlton.ingest.data_saver.log_error')
# def test_save_data_success(
#     mock_log_error,
#     mock_log_info,
#     mock_config_ingest_tgt,
#     mock_validate_args,
#     spark,
#     df,
#     config_ingest,
#     custom_config_spark,
# ):
#     """
#     Testa a função save_data para verificar se os dados são salvos corretamente.
#     Tests the save_data function to check if data is saved correctly.
#     """
#     # Mock the Spark session and DataFrame
#     mock_spark = spark
#     mock_df = df

#     # Mock the config_ingest_tgt function
#     mock_config_ingest_tgt.return_value = {
#         'checkpointLocation': '/path/to/checkpoint',
#         'path': '/path/to/table',
#         'mergeSchema': True,
#     }

#     # Call the save_data function
#     DataSaver.save_data(
#         mock_spark, mock_df, config_ingest, custom_config_spark
#     )

#     # Assertions
#     mock_validate_args.assert_any_call(
#         ['schema_name', 'table_name', 'table_path', 'type_run'], config_ingest
#     )
#     mock_config_ingest_tgt.assert_called_once_with(
#         config_ingest, custom_config_spark
#     )
#     mock_log_info.assert_any_call(
#         f'Configurations used for writing: {mock_config_ingest_tgt.return_value}'
#     )

#     # Verify table creation query
#     expected_query = f"""CREATE TABLE IF NOT EXISTS {config_ingest['schema_name']}.{config_ingest['table_name']} (,_rescued STRING,carlton_current_date DATE,carlton_metadata struct<file_path:string,file_name:string,file_size:bigint,file_block_start:bigint,file_block_length:bigint,file_modification_time:timestamp>) USING DELTA CLUSTER BY (carlton_current_date)"""
#     mock_spark.sql.assert_called_once_with(expected_query)

#     # Verify writeStream call
#     mock_df.writeStream.options.assert_called_once_with(
#         **mock_config_ingest_tgt.return_value
#     )
#     mock_df.writeStream.options.return_value.outputMode.assert_called_once_with(
#         'append'
#     )
#     mock_df.writeStream.options.return_value.outputMode.return_value.trigger.assert_called_once_with(
#         availableNow=True
#     )
#     mock_df.writeStream.options.return_value.outputMode.return_value.trigger.return_value.table.assert_called_once_with(
#         f"{config_ingest['schema_name']}.{config_ingest['table_name']}"
#     )
#     mock_df.writeStream.options.return_value.outputMode.return_value.trigger.return_value.table.return_value.awaitTermination.assert_called_once()

#     # Ensure log_error is not called
#     mock_log_error.assert_not_called()


@patch('carlton.ingest.data_saver.ConfigValidator.validate_args')
@patch('carlton.ingest.data_saver.ConfigIngestor.config_ingest_tgt')
@patch('carlton.ingest.data_saver.log_info')
@patch('carlton.ingest.data_saver.log_error')
def test_save_data_exception(
    mock_log_error,
    mock_log_info,
    mock_config_ingest_tgt,
    mock_validate_args,
    spark,
    df,
    config_ingest,
    custom_config_spark,
):
    """
    Testa a função save_data para verificar se os erros são logados corretamente.
    Tests the save_data function to check if errors are logged correctly.
    """
    # Mock the Spark session and DataFrame
    mock_spark = spark
    mock_df = df

    # Mock the config_ingest_tgt function to raise an exception
    mock_config_ingest_tgt.side_effect = Exception('Test exception')

    # Call the save_data function
    DataSaver.save_data(
        mock_spark, mock_df, config_ingest, custom_config_spark
    )

    # Check if log_error is called with the exception message
    mock_log_error.assert_called_once_with('Test exception')

    # Ensure log_info is not called
    mock_log_info.assert_not_called()
