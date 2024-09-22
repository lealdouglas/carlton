from unittest.mock import patch

import pytest

from carlton.ingest.config_validator import ConfigValidator


@patch('carlton.ingest.config_validator.Validator.validate_args')
def test_validate_args_success(mock_validate_args):
    """
    Testa a função validate_args para verificar se ela chama Validator.validate_args corretamente quando todos os parâmetros necessários estão presentes.
    Tests the validate_args function to check if it calls Validator.validate_args correctly when all required parameters are present.
    """
    required_args = ['arg1', 'arg2']
    config = {'arg1': 'value1', 'arg2': 'value2'}

    # Chama a função e verifica se Validator.validate_args foi chamada corretamente
    ConfigValidator.validate_args(required_args, config)
    mock_validate_args.assert_called_once_with(required_args, config)


@patch('carlton.ingest.config_validator.Validator.validate_args')
def test_validate_args_missing_key(mock_validate_args):
    """
    Testa a função validate_args para verificar se ela chama Validator.validate_args corretamente quando um parâmetro necessário está faltando.
    Tests the validate_args function to check if it calls Validator.validate_args correctly when a required parameter is missing.
    """
    required_args = ['arg1', 'arg2']
    config = {'arg1': 'value1'}

    # Chama a função e verifica se Validator.validate_args foi chamada corretamente
    ConfigValidator.validate_args(required_args, config)
    mock_validate_args.assert_called_once_with(required_args, config)
