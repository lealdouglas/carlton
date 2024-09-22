import pytest

from carlton.utils.validator import Validator


def test_validate_args_success():
    """
    Testa a função validate_args para verificar se ela não levanta exceções quando todos os parâmetros necessários estão presentes.
    Tests the validate_args function to check if it does not raise exceptions when all required parameters are present.
    """
    args_needed = ['table_checkpoint_location', 'table_path']
    args_user = {
        'table_checkpoint_location': '/save/_checkpointLocation',
        'table_path': '/save/',
    }

    # Chama a função e verifica se não levanta exceções
    Validator.validate_args(args_needed, args_user)


def test_validate_args_missing_key():
    """
    Testa a função validate_args para verificar se ela levanta uma exceção KeyError quando um parâmetro necessário está faltando.
    Tests the validate_args function to check if it raises a KeyError when a required parameter is missing.
    """
    args_needed = ['table_checkpoint_location', 'table_path']
    args_user = {'table_checkpoint_location': '/save/_checkpointLocation'}

    # Verifica se a função levanta uma exceção KeyError
    with pytest.raises(KeyError) as excinfo:
        Validator.validate_args(args_needed, args_user)

    # Verifica a mensagem da exceção
    assert (
        str(excinfo.value)
        == """'Nao foi possivel localizar o parametro: table_path . Por favor adicionar'"""
    )


def test_validate_args_empty_needed():
    """
    Testa a função validate_args para verificar se ela não levanta exceções quando a lista de parâmetros necessários está vazia.
    Tests the validate_args function to check if it does not raise exceptions when the list of required parameters is empty.
    """
    args_needed = []
    args_user = {
        'table_checkpoint_location': '/save/_checkpointLocation',
        'table_path': '/save/',
    }

    # Chama a função e verifica se não levanta exceções
    Validator.validate_args(args_needed, args_user)


def test_validate_args_empty_user():
    """
    Testa a função validate_args para verificar se ela levanta uma exceção KeyError quando o dicionário do usuário está vazio.
    Tests the validate_args function to check if it raises a KeyError when the user's dictionary is empty.
    """
    args_needed = ['table_checkpoint_location', 'table_path']
    args_user = {}

    # Verifica se a função levanta uma exceção KeyError
    with pytest.raises(KeyError) as excinfo:
        Validator.validate_args(args_needed, args_user)

    # Verifica a mensagem da exceção
    assert (
        str(excinfo.value)
        == """'Nao foi possivel localizar o parametro: table_checkpoint_location . Por favor adicionar'"""
    )


def test_validate_args_partial_match():
    """
    Testa a função validate_args para verificar se ela levanta uma exceção KeyError quando apenas alguns dos parâmetros necessários estão presentes.
    Tests the validate_args function to check if it raises a KeyError when only some of the required parameters are present.
    """
    args_needed = [
        'table_checkpoint_location',
        'table_path',
        'schema_location',
    ]
    args_user = {
        'table_checkpoint_location': '/save/_checkpointLocation',
        'table_path': '/save/',
    }

    # Verifica se a função levanta uma exceção KeyError
    with pytest.raises(KeyError) as excinfo:
        Validator.validate_args(args_needed, args_user)

    # Verifica a mensagem da exceção
    assert (
        str(excinfo.value)
        == """'Nao foi possivel localizar o parametro: schema_location . Por favor adicionar'"""
    )
