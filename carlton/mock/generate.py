import json
import os
import random
import time
from datetime import datetime, timedelta

from azure.eventhub import EventData, EventHubProducerClient
from azure.identity import ClientSecretCredential


# Função para enviar eventos para o Event Hub
def send_event_to_eventhub(root_properties: dict, credential, event_data):

    # Configuração do client para enviar eventos
    producer = EventHubProducerClient(
        fully_qualified_namespace=f"{root_properties['event_hub_namespace'],}.servicebus.windows.net",
        eventhub_name=root_properties['event_hub_name'],
        credential=credential,
    )
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


# Função para gerar ações de um usuário com mais interações e timestamps reais
def generate_user_actions_real_time(user_id):
    actions = [
        ('Login', True, '', ''),
        (
            'Cadastro',
            random.choice([True, False]),
            'ERR_400' if random.random() > 0.5 else '',
            'Dados inválidos' if random.random() > 0.5 else '',
        ),
        ('Consulta Saldo', True, '', ''),
        (
            'Transferência',
            random.choice([True, False]),
            'ERR_402' if random.random() > 0.5 else '',
            'Saldo insuficiente' if random.random() > 0.5 else '',
        ),
        (
            'Pagamento',
            random.choice([True, False]),
            'ERR_503' if random.random() > 0.7 else '',
            'Serviço indisponível' if random.random() > 0.7 else '',
        ),
        ('Consulta Extrato', True, '', ''),
        (
            'Atualização Perfil',
            random.choice([True, False]),
            'ERR_409' if random.random() > 0.3 else '',
            'Conflito de dados' if random.random() > 0.3 else '',
        ),
        ('Consulta Faturas', True, '', ''),
        (
            'Recarga Celular',
            random.choice([True, False]),
            'ERR_504' if random.random() > 0.4 else '',
            'Tempo de resposta excedido' if random.random() > 0.4 else '',
        ),
        ('Logout', True, '', ''),
    ]

    # Verifica se o último dígito é ímpar ou par
    if int(str(user_id)[-1]) % 2 == 0:
        phone_type = 'iPhone'
        cidade = 'Sao Paulo'
    else:
        phone_type = 'android'
        cidade = 'Belo Horizonte'

    timestamps = generate_timestamps_real_time(len(actions))
    return [
        (
            user_id,
            action,
            timestamps[i],
            phone_type,
            cidade,
            success,
            error_code,
            error_message,
        )
        for i, (action, success, error_code, error_message) in enumerate(
            actions
        )
    ]


# Simular eventos de 50 usuários em tempo real com intervalo de 5 segundos
def generate_mock_data(root_properties: dict, num_users=20, sleep_time=120):

    # Informações do Service Principal (obtidas no Azure AD)
    TENANT_ID = os.getenv('ARM_TENANT_ID', default='ARM_TENANT_ID')
    CLIENT_ID = os.getenv('ARM_CLIENT_ID', default='ARM_CLIENT_ID')
    CLIENT_SECRET = os.getenv('ARM_CLIENT_SECRET', default='ARM_CLIENT_SECRET')

    # Credenciais OAuth2 para o Service Principal
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID, client_id=CLIENT_ID, client_secret=CLIENT_SECRET
    )

    for user_id in range(100001, 100001 + num_users):
        user_actions = generate_user_actions_real_time(user_id)
        for action in user_actions:
            # Estrutura do evento
            event_data = {
                'user_id': action[0],
                'action_name': action[1],
                'action_timestamp': action[2].isoformat(),
                'device_info': action[3],
                'location': action[4],
                'success': action[5],
                'error_code': action[6],
                'error_message': action[7],
            }
            # Enviar o evento para o Event Hub
            send_event_to_eventhub(root_properties, credential, event_data)
            # Aguardar 5 segundos antes de enviar o próximo evento
        time.sleep(sleep_time)
