import json
import os
import random
import time
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from azure.eventhub import EventData, EventHubProducerClient
from azure.identity import ClientSecretCredential

from carlton.mock.generate import (
    generate_mock_data,
    generate_timestamps_real_time,
    gerar_dados_consentimentos,
    send_event_to_eventhub,
)

# Valores para gerar ações de usuários
TIPOS_ACAO = [
    'login',
    'consulta_saldo',
    'transferencia',
    'pagamento',
    'logout',
]
DISPOSITIVOS = ['Android', 'iOS']


@pytest.fixture
def mock_credential():
    """
    Fixture para mockar ClientSecretCredential.
    """
    with patch('carlton.mock.generate.ClientSecretCredential') as mock:
        yield mock


@pytest.fixture
def mock_producer_client():
    """
    Fixture para mockar EventHubProducerClient.
    """
    with patch('carlton.mock.generate.EventHubProducerClient') as mock:
        yield mock


@pytest.fixture
def mock_send_event():
    """
    Fixture para mockar send_event_to_eventhub.
    """
    with patch('carlton.mock.generate.send_event_to_eventhub') as mock:
        yield mock


def test_send_event_to_eventhub(mock_producer_client):
    """
    Testa a função send_event_to_eventhub para verificar se os eventos são enviados corretamente.
    """
    mock_producer = mock_producer_client.return_value
    root_properties = {
        'event_hub_namespace': 'test_namespace',
        'event_hub_name': 'test_eventhub',
    }
    credential = MagicMock()
    event_data = {'key': 'value'}

    send_event_to_eventhub(root_properties, credential, event_data)

    assert mock_producer.create_batch.called
    assert mock_producer.send_batch.called


def test_generate_timestamps_real_time():
    """
    Testa a função generate_timestamps_real_time para verificar se os timestamps são gerados corretamente.
    """
    num_actions = 5
    timestamps = generate_timestamps_real_time(num_actions)
    assert len(timestamps) == num_actions
    for i in range(1, num_actions):
        assert (timestamps[i] - timestamps[i - 1]).seconds == 5


def test_gerar_dados_consentimentos():
    """
    Testa a função gerar_dados_consentimentos para verificar se os consentimentos são gerados corretamente.
    """
    num_records = 5
    user_ids = range(10000, 10005)
    consentimentos = gerar_dados_consentimentos(num_records, user_ids)
    assert len(consentimentos) == num_records
    for consentimento in consentimentos:
        assert consentimento['user_id'] in user_ids
        assert consentimento['status'] in ['Ativo', 'Revogado']
        assert 'consent_id' in consentimento
        assert 'data_inicio' in consentimento
        assert 'data_fim' in consentimento
        assert 'tipo_dados' in consentimento
        assert 'plataforma_origem' in consentimento


# def test_generate_mock_data(
#     mock_send_event, mock_producer_client, mock_credential
# ):
#     """
#     Testa a função generate_mock_data para verificar se o fluxo principal é executado corretamente.
#     """
#     mock_producer = mock_producer_client.return_value
#     mock_credential.return_value = MagicMock()

#     root_properties = {
#         'event_hub_namespace': 'test_namespace',
#         'event_hub_name': 'test_eventhub',
#     }

#     generate_mock_data(root_properties, num_users=1, sleep_time=0)

#     mock_credential.assert_called_once_with(
#         tenant_id='ARM_TENANT_ID',
#         client_id='ARM_CLIENT_ID',
#         client_secret='ARM_CLIENT_SECRET',
#     )
#     mock_producer_client.assert_called_once_with(
#         fully_qualified_namespace='test_namespace.servicebus.windows.net',
#         eventhub_name='test_eventhub',
#         credential=mock_credential.return_value,
#     )
#     assert mock_send_event.called
