from unittest.mock import MagicMock, patch

import pytest

from carlton.run import run


@patch('carlton.run.SparkSession')
@patch('carlton.run.log_info')
@patch('carlton.run.log_error')
def test_run(mock_log_error, mock_log_info, mock_spark_session):
    # Mock the SparkSession.builder.getOrCreate() method
    mock_spark = MagicMock()
    mock_spark_session.builder.appName.return_value.getOrCreate.return_value = (
        mock_spark
    )

    # Call the run function
    run()

    # Check if log_info was called with 'ingestao iniciada'
    mock_log_info.assert_any_call('ingestao iniciada')

    # Check if log_info was called with 'ingestao finalizada'
    mock_log_info.assert_any_call('ingestao finalizada')

    # Ensure log_error was not called
    mock_log_error.assert_not_called()

    # Ensure SparkSession.builder.appName was called with 'Carlton Ingest APP'
    mock_spark_session.builder.appName.assert_called_with('Carlton Ingest APP')


if __name__ == '__main__':
    pytest.main()
