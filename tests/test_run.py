from unittest.mock import MagicMock, patch

import pytest

from carlton.run import create_spark_session, main, process_args


def test_process_args():
    """
    Testa a função process_args.
    Tests the process_args function.
    """
    args = ['-arg1', 'value1', '-arg2', 'value2']
    expected_output = {'arg1': 'value1', 'arg2': 'value2'}
    assert process_args(args) == expected_output


# def test_create_spark_session():
#     """
#     Testa a função create_spark_session.
#     Tests the create_spark_session function.
#     """
#     with patch(
#         'carlton.run.SparkSession.builder.getOrCreate'
#     ) as mock_get_or_create:
#         mock_spark_session = MagicMock()
#         mock_get_or_create.return_value = mock_spark_session

#         app_name = 'Test App'
#         spark_session = create_spark_session(app_name)

#         mock_get_or_create.assert_called_once()
#         assert spark_session == mock_spark_session


# @patch('carlton.run.read')
# @patch('carlton.run.save')
# @patch('carlton.run.create_spark_session')
# @patch('carlton.run.process_args')
# @patch('carlton.run.log_info')
# @patch('carlton.run.log_error')
# def test_main(
#     mock_log_error,
#     mock_log_info,
#     mock_process_args,
#     mock_create_spark_session,
#     mock_save,
#     mock_read,
# ):
#     """
#     Testa a função main.
#     Tests the main function.
#     """
#     # Configura os mocks
#     mock_process_args.return_value = {'arg1': 'value1'}
#     mock_create_spark_session.return_value = MagicMock()
#     mock_read.return_value = MagicMock()

#     # Executa a função main
#     main(['-arg1', 'value1'])

#     # Verifica se as funções foram chamadas corretamente
#     mock_log_info.assert_any_call('Ingestão iniciada')
#     mock_process_args.assert_called_once_with(['-arg1', 'value1'])
#     mock_create_spark_session.assert_called_once_with('Carlton Ingest APP')
#     mock_read.assert_called_once_with(
#         mock_create_spark_session.return_value, {'arg1': 'value1'}
#     )
#     mock_save.assert_called_once_with(
#         mock_read.return_value, {'arg1': 'value1'}
#     )
#     mock_log_info.assert_any_call('Ingestão finalizada')

#     # Simula uma exceção e verifica se log_error é chamado
#     mock_read.side_effect = Exception('Test Exception')
#     with pytest.raises(Exception):
#         main(['-arg1', 'value1'])
#     mock_log_error.assert_called_with('Test Exception')
