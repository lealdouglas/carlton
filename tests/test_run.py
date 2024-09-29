from unittest.mock import MagicMock, patch

import pytest

from carlton.run import main, process_args


@pytest.fixture
def args():
    return [
        '-schema_name',
        'test_schema',
        '-table_name',
        'test_table',
        '-table_path',
        '/path/to/table',
        '-type_run',
        'batch',
    ]


def test_process_args(args):
    """
    Testa a função process_args para verificar se os argumentos são processados corretamente.
    Tests the process_args function to check if arguments are processed correctly.
    """
    expected_output = {
        'schema_name': 'test_schema',
        'table_name': 'test_table',
        'table_path': '/path/to/table',
        'type_run': 'batch',
    }
    assert process_args(args) == expected_output


@patch('carlton.run.log_info')
@patch('carlton.run.log_error')
@patch('carlton.run.read')
@patch('carlton.run.save')
@patch('carlton.run.SparkSessionManager.create_spark_session')
def test_main_success(
    mock_create_spark_session,
    mock_save,
    mock_read,
    mock_log_error,
    mock_log_info,
    args,
):
    """
    Testa a função main para verificar se a ingestão de dados é executada corretamente.
    Tests the main function to check if data ingestion is executed correctly.
    """
    # Mock the Spark session and DataFrame
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_create_spark_session.return_value = mock_spark
    mock_read.return_value = mock_df

    # Call the main function
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
            'table_path': '/path/to/table',
            'type_run': 'batch',
        },
    )
    mock_save.assert_called_once_with(
        mock_spark,
        mock_df,
        {
            'schema_name': 'test_schema',
            'table_name': 'test_table',
            'table_path': '/path/to/table',
            'type_run': 'batch',
        },
    )
    mock_log_error.assert_not_called()


@patch('carlton.run.log_info')
@patch('carlton.run.log_error')
@patch('carlton.run.read')
@patch('carlton.run.save')
@patch('carlton.run.SparkSessionManager.create_spark_session')
def test_main_exception(
    mock_create_spark_session,
    mock_save,
    mock_read,
    mock_log_error,
    mock_log_info,
    args,
):
    """
    Testa a função main para verificar se os erros são logados corretamente.
    Tests the main function to check if errors are logged correctly.
    """
    # Mock the Spark session and DataFrame
    mock_spark = MagicMock()
    mock_create_spark_session.return_value = mock_spark

    # Mock the read function to raise an exception
    mock_read.side_effect = Exception('Test exception')

    # Call the main function
    with pytest.raises(Exception):
        main(args)

    # Check if log_error is called with the exception message
    mock_log_error.assert_called_once_with('Test exception')

    # Ensure log_info is called for the start but not for the end
    mock_log_info.assert_any_call('Ingestão iniciada')
    mock_log_info.assert_called_with('type_run: batch')
