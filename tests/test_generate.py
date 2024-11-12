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


@pytest.fixture
def root_properties():
    return {
        'event_hub_namespace': 'test_namespace',
        'event_hub_name': 'test_eventhub',
        'sleep_time': 5,
    }


@patch('carlton.mock.generate.ClientSecretCredential')
@patch('carlton.mock.generate.EventHubProducerClient')
@patch('carlton.mock.generate.log_info')
@patch('carlton.mock.generate.log_error')
@patch('carlton.mock.generate.send_event_to_eventhub')
def test_generate_mock_data(
    mock_send_event,
    mock_log_error,
    mock_log_info,
    mock_producer_client,
    mock_credential,
    root_properties,
):
    # Mock the methods
    mock_credential.return_value = MagicMock()
    mock_producer = mock_producer_client.return_value

    # Call the method
    generate_mock_data(root_properties)

    # Assertions
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
    mock_log_info.assert_any_call('test_namespace.servicebus.windows.net')
    mock_log_info.assert_any_call('test_eventhub')
    mock_send_event.assert_called_once()
    mock_log_error.assert_not_called()


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
    consentimentos = gerar_dados_consentimentos()
    assert len(consentimentos) == 21  # 10000 to 10020 inclusive
    for consentimento in consentimentos:
        assert 'consent_id' in consentimento
        assert 'user_id' in consentimento
        assert 'data_inicio' in consentimento
        assert 'data_fim' in consentimento
        assert 'tipo_dados' in consentimento
        assert 'tipo_id' in consentimento
        assert 'status' in consentimento
        assert 'plataforma_origem' in consentimento


@patch('carlton.mock.generate.EventHubProducerClient')
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

    send_event_to_eventhub(
        root_properties, mock_producer, credential, event_data
    )

    mock_producer.create_batch.assert_called_once()
    mock_producer.send_batch.assert_called_once()
    mock_producer.close.assert_called_once()
