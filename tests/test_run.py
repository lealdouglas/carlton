from unittest.mock import MagicMock, patch

import pytest

from carlton.run import main, process_args


@pytest.fixture
def args():
    return [
        'ingest',
        '-function',
        'ingest',
        '-storage_name_src',
        'stadrisk',
        '-container_src',
        'ctrdriskraw',
        '-file_resource',
        'adls',
        '-type_run',
        'batch',
        '-storage_name_tgt',
        'stadrisk',
        '-container_tgt',
        'dtmaster-catalog',
        '-schema_name',
        'bronze',
        '-table_name',
        'table_test',
        '-file_extension',
        'csv',
        '-path_src',
        'table_test',
        '-file_header',
        'true',
        '-file_delimiter',
        ',',
    ]


def test_process_args(args):
    """
    Testa a função process_args para verificar se os argumentos são processados corretamente.
    Tests the process_args function to check if arguments are processed correctly.
    """
    expected_output = {
        'function': 'ingest',
        'storage_name_src': 'stadrisk',
        'container_src': 'ctrdriskraw',
        'file_resource': 'adls',
        'type_run': 'batch',
        'storage_name_tgt': 'stadrisk',
        'container_tgt': 'dtmaster-catalog',
        'schema_name': 'bronze',
        'table_name': 'table_test',
        'file_extension': 'csv',
        'path_src': 'table_test',
        'file_header': 'true',
        'file_delimiter': ',',
    }
    assert process_args(args) == expected_output


@patch('carlton.run.log_error')
@patch('carlton.run.log_info')
@patch('carlton.run.read')
@patch('carlton.run.SparkSessionManager.create_spark_session')
def test_main(
    mock_create_spark_session, mock_read, mock_log_info, mock_log_error, args
):
    """
    Testa a função principal para verificar se as funções são chamadas corretamente.
    Tests the main function to check if functions are called correctly.
    """
    # Mock the SparkSession
    mock_spark = MagicMock()
    mock_create_spark_session.return_value = mock_spark

    # Mock the DataFrame
    mock_df = MagicMock()
    mock_read.return_value = mock_df

    # Call the main function
    main(args)

    # Check if log_info is called with 'Ingestão iniciada'
    mock_log_info.assert_any_call('Ingestão iniciada')

    # Check if log_info is called with root properties
    root_properties = process_args(args)
    for p in root_properties:
        mock_log_info.assert_any_call(f'{p}: {root_properties[p]}')

    # Check if SparkSession is created
    mock_create_spark_session.assert_called_once_with('Carlton Ingest APP')

    # Check if read function is called with correct arguments
    mock_read.assert_called_once_with(mock_spark, root_properties)

    # Check if log_info is called with 'Ingestão finalizada'
    mock_log_info.assert_any_call('Ingestão finalizada')

    # Ensure log_error is not called
    mock_log_error.assert_not_called()


@patch('carlton.run.log_error')
@patch('carlton.run.log_info')
@patch('carlton.run.read')
@patch('carlton.run.SparkSessionManager.create_spark_session')
def test_main_exception(
    mock_create_spark_session, mock_read, mock_log_info, mock_log_error, args
):
    """
    Testa a função principal para verificar se os erros são logados corretamente.
    Tests the main function to check if errors are logged correctly.
    """
    # Mock the SparkSession to raise an exception
    mock_create_spark_session.side_effect = Exception('Test exception')

    # Call the main function
    main(args)

    # Check if log_error is called with the exception message
    mock_log_error.assert_called_once_with('Test exception')
