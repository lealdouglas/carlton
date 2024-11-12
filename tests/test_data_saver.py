from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import DataFrame, SparkSession

from carlton.ingest.config_ingestor import ConfigIngestor
from carlton.ingest.config_validator import ConfigValidator
from carlton.ingest.data_saver import DataSaver
from carlton.utils.logger import log_error, log_info


@pytest.fixture
def spark():
    return SparkSession.builder.master('local').appName('test').getOrCreate()


@pytest.fixture
def df(spark):
    data = [('Alice', 1), ('Bob', 2)]
    columns = ['name', 'value']
    return spark.createDataFrame(data, columns)


@pytest.fixture
def config_ingest():
    return {
        'schema_name': 'default',
        'table_name': 'example_table',
        'table_path': '/path/to/table',
        'type_run': 'batch',
        'trigger_processing_time': '10 seconds',
        'file_extension': 'parquet',
    }


@pytest.fixture
def custom_config_spark():
    return {'spark.some.config.option': 'some-value'}


@patch('carlton.ingest.data_saver.ConfigValidator')
@patch('carlton.ingest.data_saver.ConfigIngestor')
@patch('carlton.ingest.data_saver.log_info')
@patch('carlton.ingest.data_saver.log_error')
@patch('pyspark.sql.DataFrame.writeStream', new_callable=MagicMock)
def test_save_data(
    mock_write_stream,
    mock_log_error,
    mock_log_info,
    mock_config_ingestor,
    mock_config_validator,
    spark,
    df,
    config_ingest,
    custom_config_spark,
):
    # Mock the methods
    mock_config_validator.validate_args = MagicMock()
    mock_config_ingestor.config_ingest_tgt = MagicMock(
        return_value={'path': '/path/to/save'}
    )

    # Mock the writeStream method chain
    mock_write_stream_instance = mock_write_stream.return_value

    # Call the method
    DataSaver.save_data(spark, df, config_ingest, custom_config_spark)

    # Assertions
    mock_config_validator.validate_args.assert_any_call(
        ['schema_name', 'table_name', 'table_path', 'type_run'],
        config_ingest,
    )
    mock_config_ingestor.config_ingest_tgt.assert_called_once_with(
        config_ingest, custom_config_spark
    )
    mock_log_info.assert_any_call(
        f"Configurations used for writing: {{'path': '/path/to/save'}}"
    )
    mock_log_info.assert_any_call(
        f"Registering table {config_ingest['table_name']} in schema {config_ingest['schema_name']}"
    )
    mock_log_info.assert_any_call(
        f'Using liquid_cluster. Managed by the column carlton_current_date'
    )
    mock_log_info.assert_any_call(
        f'Registering table: CREATE TABLE IF NOT EXISTS {config_ingest["schema_name"]}.{config_ingest["table_name"]} (name STRING, value STRING,_rescued STRING,carlton_current_date DATE,carlton_metadata struct<file_path:string,file_name:string,file_size:bigint,file_block_start:bigint,file_block_length:bigint,file_modification_time:timestamp>) USING DELTA CLUSTER BY (carlton_current_date)'
    )
