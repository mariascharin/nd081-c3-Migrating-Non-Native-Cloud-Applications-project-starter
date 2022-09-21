import logging
import os
import azure.functions as func
from azure.servicebus import ServiceBusClient, ServiceBusReceiveMode
from .. import settings

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger was executed.')
    # Get queue information
    connection_string = settings.SERVICE_BUS_CONNECTION_STR
    queue_name = settings.SERVICE_BUS_QUEUE_NAME

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        logging.info('We have some input')
        with ServiceBusClient.from_connection_string(connection_string) as client:
            # max_wait_time specifies how long the receiver should wait with no incoming messages before stopping receipt.
            # Default is None; to receive forever.
            with client.get_queue_receiver(queue_name, max_wait_time=10, receive_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE) as receiver:
                for msg in receiver:  # ServiceBusReceiver instance is a generator.
                    logging.info("Received the message: %s" %(str(msg)))
                    # If it is desired to halt receiving early, one can break out of the loop here safely.

        return func.HttpResponse("This HTTP triggered function executed successfully and handled the queue.",
            status_code=200
        )
        
    else:
        logging.info('No input')
        return func.HttpResponse(
            "This HTTP triggered function executed successfully but without accessing the queue.",
            status_code=200
    )
