from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame

from carlton.ingest.data_saver import DataSaver


@pytest.fixture
def config_ingest():
    return {
        'schema_name': 'default',
        'table_name': 'example_table',
        'table_path': '/path/to/table',
        'type_run': 'batch',
        'trigger_processing_time': '10 seconds',
    }


@pytest.fixture
def custom_config_spark():
    return {'spark.some.config.option': 'some-value'}


@pytest.fixture
def df():
    spark = MagicMock()
    data = [('Alice', 1), ('Bob', 2)]
    columns = ['name', 'value']
    return spark.createDataFrame(data, columns)


# @patch('carlton.ingest.data_saver.ConfigValidator.validate_args')
# @patch('carlton.ingest.data_saver.ConfigIngestor.config_ingest_tgt')
# @patch(
#     'carlton.ingest.data_saver.SparkSession.builder.getOrCreate',
#     return_value=MagicMock(),
# )
# @patch('carlton.ingest.data_saver.log_info')
# @patch('carlton.ingest.data_saver.log_error')
# def test_save_data(
#     mock_log_error,
#     mock_log_info,
#     mock_getOrCreate,
#     mock_config_ingest_tgt,
#     mock_validate_args,
#     df,
#     config_ingest,
#     custom_config_spark,
# ):
#     """
#     Testa a função save_data para verificar se os dados são salvos corretamente.
#     Tests the save_data function to check if data is saved correctly.
#     """
#     # Mock the Spark session and DataFrame
#     mock_spark = mock_getOrCreate.return_value
#     mock_df = df
#     mock_spark.createDataFrame.return_value = mock_df

#     # Mock the config_ingest_tgt function
#     mock_config_ingest_tgt.return_value = {
#         'checkpointLocation': '/path/to/checkpoint',
#         'path': '/path/to/table',
#         'mergeSchema': True,
#     }

#     # Call the save_data function
#     DataSaver.save_data(mock_df, config_ingest, custom_config_spark)

#     # Assertions
#     mock_validate_args.assert_any_call(
#         ['schema_name', 'table_name', 'table_path', 'type_run'], config_ingest
#     )
#     mock_config_ingest_tgt.assert_called_once_with(
#         config_ingest, custom_config_spark
#     )
#     mock_log_info.assert_any_call(
#         'Configurations used for writing: ',
#         msg_dict=mock_config_ingest_tgt.return_value,
#     )
#     mock_getOrCreate.assert_called_once()

#     # Verify table creation query
#     expected_query = f"""
#         CREATE TABLE IF NOT EXISTS {config_ingest['schema_name']}.{config_ingest['table_name']} (
#         name STRING, value STRING
#         ,_rescued STRING
#         ,carlton_current_date DATE
#         ,carlton_metadata struct<file_path:string,file_name:string,file_size:bigint,file_block_start:bigint,file_block_length:bigint,file_modification_time:timestamp>
#         )
#         USING DELTA CLUSTER BY (carlton_current_date) LOCATION '{config_ingest['table_path']}'"""
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

#     # Test exception handling
#     mock_df.writeStream.options.return_value.outputMode.return_value.trigger.return_value.table.return_value.awaitTermination.side_effect = Exception(
#         'Test exception'
#     )
#     DataSaver.save_data(mock_df, config_ingest, custom_config_spark)
#     mock_log_error.assert_called_once_with('Test exception')
