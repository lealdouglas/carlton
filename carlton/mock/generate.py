import json
import os
import random
import time
from datetime import datetime, timedelta

from azure.eventhub import EventData, EventHubProducerClient
from azure.identity import ClientSecretCredential

from carlton.utils.logger import log_error, log_info


# Função para enviar eventos para o Event Hub
def send_event_to_eventhub(
    root_properties: dict,
    producer: EventHubProducerClient,
    credential,
    event_data,
):

    try:
        # Criar um lote de eventos
        event_batch = producer.create_batch()
        # Adicionar o evento ao lote
        event_batch.add(EventData(json.dumps(event_data)))
        # Enviar o lote
        producer.send_batch(event_batch)
        print(f'Evento enviado: {event_data}')
    except Exception as ex:
        print(f'Erro ao enviar evento: {ex}')
    finally:
        print('Evento enviado com sucesso!')
        producer.close()


# Função para gerar timestamps incrementais com intervalo de 5 segundos
def generate_timestamps_real_time(num_actions):
    now = datetime.now()
    timestamps = [now + timedelta(seconds=5 * i) for i in range(num_actions)]
    return timestamps


# Função para gerar consentimentos
def gerar_dados_consentimentos():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)
    data_types = [
        'Dados Financeiros',
        'Dados de Credito',
        'Dados Pessoais',
        'Dados de Saude',
        'Dados de Localizacao',
        'Dados de Navegacao',
        'Dados de Compras',
        'Dados de Contato',
        'Dados de Publicidade',
        'Dados de Historico de Navegacao',
    ]
    data_type_map = {
        data_type: str(idx).zfill(3)
        for idx, data_type in enumerate(data_types, start=1)
    }

    consent_data = []

    for i in range(10000, 10020 + 1):
        consent_id = f"c{random.choice(['d', 't', 'm'])}{random.choice(range(100, 9999))}"
        user_id = i
        data_inicio = start_date + timedelta(
            days=random.randint(0, (end_date - start_date).days)
        )
        data_fim = data_inicio + timedelta(
            days=random.randint(1, 365)
        )  # Garantir fim após o início
        tipo_dados = random.choice(data_types)
        tipo_id = data_type_map[tipo_dados]
        plataforma_origem = random.choice(['Web', 'Mobile', 'API'])
        status = random.choice(['Ativo', 'Revogado'])

        # Registro "Ativo"
        consent_data.append(
            {
                'consent_id': consent_id,
                'user_id': user_id,
                'data_inicio': data_inicio.strftime('%Y-%m-%d')
                if status == 'Ativo'
                else '',
                'data_fim': data_fim.strftime('%Y-%m-%d')
                if status == 'Ativo'
                else '',
                'tipo_dados': tipo_dados,
                'tipo_id': tipo_id,
                'status': status,
                'plataforma_origem': plataforma_origem,
            }
        )

    return consent_data


# Simular eventos de 50 usuários em tempo real com intervalo de 5 segundos
def generate_mock_data(root_properties: dict):

    # Informações do Service Principal (obtidas no Azure AD)
    TENANT_ID = os.getenv('ARM_TENANT_ID', default='ARM_TENANT_ID')
    CLIENT_ID = os.getenv('ARM_CLIENT_ID', default='ARM_CLIENT_ID')
    CLIENT_SECRET = os.getenv('ARM_CLIENT_SECRET', default='ARM_CLIENT_SECRET')

    # Credenciais OAuth2 para o Service Principal
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
    )

    # Gerar ações de usuários em tempo real
    dados_consentimentos = gerar_dados_consentimentos()

    # Configuração do client para enviar eventos
    fully_qualified_namespace = (
        f"{root_properties['event_hub_namespace']}.servicebus.windows.net"
    )
    eventhub_name = root_properties['event_hub_name']

    log_info(fully_qualified_namespace)
    log_info(eventhub_name)
    producer = EventHubProducerClient(
        fully_qualified_namespace=fully_qualified_namespace,
        eventhub_name=eventhub_name,
        credential=credential,
    )

    # for consentimento in dados_consentimentos:

    # Enviar o evento para o Event Hub
    send_event_to_eventhub(
        root_properties, producer, credential, consentimento
    )

    # Aguardar 5 segundos antes de enviar o próximo evento
    # time.sleep(root_properties['sleep_time'])
