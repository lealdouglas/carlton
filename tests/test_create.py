import os
from unittest.mock import MagicMock, patch

import pytest

from carlton.ddl.create import create, get_absolute_path


def test_get_absolute_path_no_dbutils():
    """
    Testa a função get_absolute_path quando dbutils não está disponível.
    Tests the get_absolute_path function when dbutils is not available.
    """
    relative_path = ('/jarvis/prep/datacontract/', 'datacontract.yaml')
    expected_path = os.path.join(*relative_path)
    assert get_absolute_path(*relative_path) == expected_path


@patch('carlton.ddl.create.dbutils', create=True)
def test_get_absolute_path_with_dbutils(mock_dbutils):
    """
    Testa a função get_absolute_path quando dbutils está disponível.
    Tests the get_absolute_path function when dbutils is available.
    """
    mock_dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get.return_value = (
        '/Workspace/Projects/Notebook'
    )
    relative_path = ('/jarvis/prep/datacontract/', 'datacontract.yaml')
    expected_path = '/Workspace\jarvis\prep\datacontract\datacontract.yaml'
    assert get_absolute_path(*relative_path) == expected_path


@patch('carlton.ddl.create.get_absolute_path')
@patch('carlton.ddl.create.DataContract')
def test_create(mock_data_contract, mock_get_absolute_path):
    """
    Testa a função create para verificar se ela executa corretamente.
    Tests the create function to check if it executes correctly.
    """
    mock_spark = MagicMock()
    mock_get_absolute_path.return_value = '/absolute/path/to/datacontract.yaml'
    mock_data_contract_instance = mock_data_contract.return_value
    mock_data_contract_instance.export.return_value = 'CREATE TABLE ...'

    create(mock_spark)

    mock_get_absolute_path.assert_called_once_with(
        '/jarvis/prep/datacontract/', 'datacontract.yaml'
    )
    mock_data_contract.assert_called_once_with(
        data_contract_file='/absolute/path/to/datacontract.yaml'
    )
    mock_data_contract_instance.export.assert_called_once_with(
        export_format='sql'
    )
    # mock_spark.sql.assert_called_once_with('CREATE TABLE ...')
