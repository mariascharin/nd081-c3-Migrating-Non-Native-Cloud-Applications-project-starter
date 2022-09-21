import logging
import os
import azure.functions as func
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from .. import settings


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    # Get queue information
    connection_string = settings.SERVICE_BUS_CONNECTION_STR
    queue_name = settings.SERVICE_BUS_QUEUE_NAME
    notification_id = req.params.get('notification_id')
    if not notification_id:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            notification_id = req_body.get('notification_id')
    logging.info("The notification id is: %s" %(str(notification_id)))
    logging.info("The connection_string is: %s" %(str(connection_string)))
    logging.info("The queue_name is: %s" %(str(queue_name)))
    if notification_id:
        with ServiceBusClient.from_connection_string(connection_string) as client:
            with client.get_queue_sender(queue_name) as sender:
                # Sending a single message
                single_message = ServiceBusMessage("18")
                sender.send_messages(single_message)
        return func.HttpResponse(f"This HTTP triggered function executed successfully with notification_id set to {notification_id}.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a notification_id in the query string or in the request body for a customized response.",
             status_code=200
        )
