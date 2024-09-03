import datetime
import json
import os
import random
import time
import uuid

from azure.eventhub import EventData, EventHubProducerClient

from carlton.utils.logger import (  # Certifique-se de importar o log_error e log_info
    log_error,
    log_info,
)


def exec(conn: str, namespace_name: str):
    try:
        EVENT_HUB_CONNECTION_STR = conn
        EVENT_HUB_NAME = namespace_name

        # This script simulates the production of events for 10 devices.
        accounts = []
        for x in range(0, 10):
            accounts.append(str(uuid.uuid4()))

        # Create a producer client to produce and publish events to the event hub.
        producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
        )

        for y in range(0, 20):    # For each device, produce 20 events.
            try:
                event_data_batch = (
                    producer.create_batch()
                )   # Create a batch. You will add events to the batch later.
                for acc in accounts:
                    try:
                        # Create a dummy reading.
                        reading = {
                            'account_id': acc,
                            'risk_score': random.randint(0, 100),
                            'risk_level': random.choice(
                                ['low', 'medium', 'high', 'error']
                            ),
                            'assessment_date': '1994-07-27',
                        }
                        s = json.dumps(
                            reading
                        )   # Convert the reading into a JSON string.
                        event_data_batch.add(
                            EventData(s)
                        )   # Add event data to the batch.
                    except Exception as e:
                        log_error(
                            f'Error creating or adding event data: {str(e)}'
                        )
                producer.send_batch(
                    event_data_batch
                )   # Send the batch of events to the event hub.
            except Exception as e:
                log_error(
                    f'Error creating or sending event data batch: {str(e)}'
                )

        # Close the producer.
        producer.close()
    except Exception as e:
        log_error(f'Error in exec function: {str(e)}')
