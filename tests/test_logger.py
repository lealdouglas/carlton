from unittest.mock import patch

import pytest

from carlton.utils.logger import log_debug, log_error, log_info, log_warning


@patch('carlton.utils.logger.logger')
def test_log_info(mock_logger):
    """
    Testa a função log_info para verificar se a mensagem de informação é logada corretamente.
    Tests the log_info function to check if the info message is logged correctly.
    """
    message = 'This is an info message'
    log_info(message)
    mock_logger.info.assert_called_once_with(message)


@patch('carlton.utils.logger.logger')
def test_log_warning(mock_logger):
    """
    Testa a função log_warning para verificar se a mensagem de aviso é logada corretamente.
    Tests the log_warning function to check if the warning message is logged correctly.
    """
    message = 'This is a warning message'
    log_warning(message)
    mock_logger.warning.assert_called_once_with(message)


@patch('carlton.utils.logger.logger')
def test_log_error(mock_logger):
    """
    Testa a função log_error para verificar se a mensagem de erro é logada corretamente.
    Tests the log_error function to check if the error message is logged correctly.
    """
    message = 'This is an error message'
    log_error(message)
    mock_logger.error.assert_called_once_with(message)


@patch('carlton.utils.logger.logger')
def test_log_debug(mock_logger):
    """
    Testa a função log_debug para verificar se a mensagem de depuração é logada corretamente.
    Tests the log_debug function to check if the debug message is logged correctly.
    """
    message = 'This is a debug message'
    log_debug(message)
    mock_logger.debug.assert_called_once_with(message)
