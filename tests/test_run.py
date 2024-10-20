from unittest.mock import MagicMock, patch

import pytest

from carlton.run import main, process_args


def test_process_args():
    """
    Testa a função process_args para verificar se os argumentos são processados corretamente.
    """
    args = [
        '-schema_name',
        'test_schema',
        '-table_name',
        'test_table',
        '-function',
        'ingest',
    ]
    expected_output = {
        'schema_name': 'test_schema',
        'table_name': 'test_table',
        'function': 'ingest',
    }
    assert process_args(args) == expected_output


@patch('carlton.run.log_info')
@patch('carlton.run.log_error')
@patch('carlton.run.read')
@patch('carlton.run.save')
@patch('carlton.run.create')
@patch('carlton.run.generate_mock_data')
@patch('carlton.run.SparkSessionManager.create_spark_session')
def test_main_ingest(
    mock_create_spark_session,
    mock_generate_mock_data,
    mock_create,
    mock_save,
    mock_read,
    mock_log_error,
    mock_log_info,
):
    """
    Testa a função main para verificar se a ingestão de dados é executada corretamente.
    """
    # Mock the Spark session and DataFrame
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_create_spark_session.return_value = mock_spark
    mock_read.return_value = mock_df

    # Call the main function with ingest function
    args = [
        '-schema_name',
        'test_schema',
        '-table_name',
        'test_table',
        '-function',
        'ingest',
    ]
    main(args)

    # Assertions
    mock_log_info.assert_any_call('Ingestão iniciada')
    mock_log_info.assert_any_call('Ingestão finalizada')
    mock_create_spark_session.assert_called_once_with('Carlton Ingest APP')
    mock_read.assert_called_once_with(
        mock_spark,
        {
            'schema_name': 'test_schema',
            'table_name': 'test_table',
            'function': 'ingest',
        },
    )
    mock_save.assert_called_once_with(
        mock_spark,
        mock_df,
        {
            'schema_name': 'test_schema',
            'table_name': 'test_table',
            'function': 'ingest',
        },
    )
    mock_log_error.assert_not_called()


@patch('carlton.run.log_info')
@patch('carlton.run.log_error')
@patch('carlton.run.read')
@patch('carlton.run.save')
@patch('carlton.run.create')
@patch('carlton.run.generate_mock_data')
@patch('carlton.run.SparkSessionManager.create_spark_session')
def test_main_create_table(
    mock_create_spark_session,
    mock_generate_mock_data,
    mock_create,
    mock_save,
    mock_read,
    mock_log_error,
    mock_log_info,
):
    """
    Testa a função main para verificar se a criação de tabela é executada corretamente.
    """
    # Mock the Spark session
    mock_spark = MagicMock()
    mock_create_spark_session.return_value = mock_spark

    # Call the main function with create_table function
    args = [
        '-schema_name',
        'test_schema',
        '-table_name',
        'test_table',
        '-function',
        'create_table',
    ]
    main(args)

    # Assertions
    mock_create_spark_session.assert_called_once_with('Carlton Ingest APP')
    mock_create.assert_called_once_with(mock_spark)
    mock_log_error.assert_not_called()


@patch('carlton.run.log_info')
@patch('carlton.run.log_error')
@patch('carlton.run.read')
@patch('carlton.run.save')
@patch('carlton.run.create')
@patch('carlton.run.generate_mock_data')
@patch('carlton.run.SparkSessionManager.create_spark_session')
def test_main_mock_data(
    mock_create_spark_session,
    mock_generate_mock_data,
    mock_create,
    mock_save,
    mock_read,
    mock_log_error,
    mock_log_info,
):
    """
    Testa a função main para verificar se a geração de dados mock é executada corretamente.
    """
    # Mock the Spark session
    mock_spark = MagicMock()
    mock_create_spark_session.return_value = mock_spark

    # Call the main function with mock_data function
    args = [
        '-schema_name',
        'test_schema',
        '-table_name',
        'test_table',
        '-function',
        'mock_data',
    ]
    main(args)

    # Assertions
    mock_create_spark_session.assert_called_once_with('Carlton Ingest APP')
    mock_generate_mock_data.assert_called_once_with(
        {
            'schema_name': 'test_schema',
            'table_name': 'test_table',
            'function': 'mock_data',
        }
    )
    mock_log_error.assert_not_called()
