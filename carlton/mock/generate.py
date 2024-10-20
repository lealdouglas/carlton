import json
import os
import random
from datetime import datetime

from azure.eventhub import EventData, EventHubProducerClient
from azure.identity import ClientSecretCredential


# Função para gerar dados aleatórios de ações do usuário
def gerar_acao_usuario(id_usuario, TIPOS_ACAO, DISPOSITIVOS):
    return {
        'id_acao': f'a{id_usuario}_{random.randint(1000, 9999)}',  # ID único para cada ação
        'id_usuario': f'u{id_usuario}',  # Identificador do usuário
        'tipo_acao': random.choice(TIPOS_ACAO),  # Tipo de ação aleatório
        'data_hora_acao': datetime.utcnow().isoformat(),  # Data e hora atuais
        'dispositivo': random.choice(DISPOSITIVOS),  # Dispositivo aleatório
        'sucesso': random.choice([True, False]),  # Sucesso ou falha aleatório
    }


def generate_mock_data(root_properties: dict):

    qtd_usuarios = 20

    # Informações do Service Principal (obtidas no Azure AD)
    TENANT_ID = os.getenv('ARM_TENANT_ID', default='ARM_TENANT_ID')
    CLIENT_ID = os.getenv('ARM_CLIENT_ID', default='ARM_CLIENT_ID')
    CLIENT_SECRET = os.getenv('ARM_CLIENT_SECRET', default='ARM_CLIENT_SECRET')

    # Informações do Event Hub
    EVENT_HUB_NAMESPACE = root_properties[
        'event_hub_namespace'
    ]  # Nome do namespace do Event Hub
    EVENT_HUB_NAME = root_properties[
        'event_hub_name'
    ]  # Nome do Event Hub específico

    # Credenciais OAuth2 para o Service Principal
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
    )

    # Configuração do client para enviar eventos
    producer = EventHubProducerClient(
        fully_qualified_namespace=f'{EVENT_HUB_NAMESPACE}.servicebus.windows.net',
        eventhub_name=EVENT_HUB_NAME,
        credential=credential,
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

    # Envia ações de 20 usuários para o Event Hub
    with producer:
        event_batch = producer.create_batch()

        for id_usuario in range(1, qtd_usuarios + 1):
            acao_usuario = gerar_acao_usuario(
                id_usuario, TIPOS_ACAO, DISPOSITIVOS
            )
            # Converte os dados para o formato JSON
            event_data = EventData(json.dumps(acao_usuario))
            event_batch.add(event_data)

        # Envia o lote de eventos para o Event Hub
        producer.send_batch(event_batch)

    print(f'{20} ações de usuários enviadas com sucesso!')
