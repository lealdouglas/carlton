from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

from carlton.utils.spark_session_manager import SparkSessionManager

# @patch('carlton.utils.spark_session_manager.SparkSession.builder.getOrCreate')
# def test_get_spark_session(mock_get_or_create):
#     """
#     Testa a função get_spark_session para verificar se a sessão Spark é obtida ou criada corretamente.
#     Tests the get_spark_session function to check if the Spark session is obtained or created correctly.
#     """
#     # Mock the SparkSession
#     mock_spark_session = MagicMock(spec=SparkSession)
#     mock_get_or_create.return_value = mock_spark_session

#     # Call the get_spark_session function
#     result = SparkSessionManager.get_spark_session()

#     # Assertions
#     mock_get_or_create.assert_called_once()
#     assert result == mock_spark_session


# @patch('carlton.utils.spark_session_manager.SparkSession.builder.appName')
# def test_create_spark_session(mock_app_name):
#     """
#     Testa a função create_spark_session para verificar se a sessão Spark é criada ou obtida corretamente com o nome do aplicativo.
#     Tests the create_spark_session function to check if the Spark session is created or obtained correctly with the application name.
#     """
#     # Mock the SparkSession
#     mock_spark_session = MagicMock(spec=SparkSession)
#     mock_builder = MagicMock()
#     mock_builder.getOrCreate.return_value = mock_spark_session
#     mock_app_name.return_value = mock_builder

#     app_name = 'TestApp'

#     # Call the create_spark_session function
#     result = SparkSessionManager.create_spark_session(app_name)

#     # Assertions
#     mock_app_name.assert_called_once_with(app_name)
#     mock_builder.getOrCreate.assert_called_once()
#     assert result == mock_spark_session
