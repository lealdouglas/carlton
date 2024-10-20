import json
import os
import random
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from azure.eventhub import EventData, EventHubProducerClient
from azure.identity import ClientSecretCredential

from carlton.mock.generate import generate_mock_data, gerar_acao_usuario

# Valores para gerar ações de usuários
TIPOS_ACAO = [
    'login',
    'consulta_saldo',
    'transferencia',
    'pagamento',
    'logout',
]
DISPOSITIVOS = ['Android', 'iOS']


def test_gerar_acao_usuario():
    """
    Testa a função gerar_acao_usuario para verificar se os dados são gerados corretamente.
    """
    id_usuario = 1
    acao_usuario = gerar_acao_usuario(id_usuario, TIPOS_ACAO, DISPOSITIVOS)
    assert acao_usuario['id_usuario'] == f'u{id_usuario}'
    assert acao_usuario['tipo_acao'] in TIPOS_ACAO
    assert acao_usuario['dispositivo'] in DISPOSITIVOS
    assert isinstance(acao_usuario['sucesso'], bool)
    assert 'data_hora_acao' in acao_usuario


@patch('carlton.mock.generate.ClientSecretCredential')
@patch('carlton.mock.generate.EventHubProducerClient')
def test_generate_mock_data(mock_producer_client, mock_credential):
    """
    Testa a função generate_mock_data para verificar se o fluxo principal é executado corretamente.
    """
    mock_producer = mock_producer_client.return_value
    mock_credential.return_value = MagicMock()

    root_properties = {
        'event_hub_namespace': 'test_namespace',
        'event_hub_name': 'test_eventhub',
    }

    generate_mock_data(root_properties)

    mock_credential.assert_called_once_with(
        tenant_id='ARM_TENANT_ID',
        client_id='ARM_CLIENT_ID',
        client_secret='ARM_CLIENT_SECRET',
    )
    mock_producer_client.assert_called_once_with(
        fully_qualified_namespace='test_namespace.servicebus.windows.net',
        eventhub_name='test_eventhub',
        credential=mock_credential.return_value,
    )
    assert mock_producer.create_batch.called
    assert mock_producer.send_batch.called
