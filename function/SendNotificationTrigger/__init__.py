import azure.functions as func
import logging
import psycopg2
from psycopg2 import Error
import os
from datetime import datetime
from .. import settings
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


def main(msg: func.ServiceBusMessage):
    notification_id = msg.get_body().decode('utf-8')
    logging.info('Python ServiceBus queue trigger processed message: %s', notification_id)

    # Update connection string information
    host = settings.POSTGRES_SERVER_NAME
    dbname = settings.POSTGRES_DB
    user = settings.POSTGRES_USER
    password = settings.POSTGRES_PW
    sslmode = "require"

    # Construct connection string
    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

    # Variables
    nbr_attendees = 0
    email_subject = ""
    email_body = ""
    notification_date = datetime.utcnow()

    # Connect to DB
    try:
        conn = psycopg2.connect(conn_string)
        logging.info("Connection established")
        cursor = conn.cursor()
    except (Exception, Error) as error:
        logging.info("Error while connecting to PostgreSQL", error)
        if (conn):
            cursor.close()
            conn.close()
    
    # Get notification message and subject from database using the notification_id
    logging.info("notification_id: %s", str(notification_id))
    try:
        cursor.execute("SELECT subject, message FROM notification where id = %s;", (notification_id,))
        row = cursor.fetchone()
        email_subject = str(row[0])
        email_message = str(row[1])

    except (Exception, Error) as error:
        logging.info("Error while selecting data", error)
        if (conn):
            cursor.close()
            conn.close()
            logging.info("PostgreSQL connection is closed in select notification block")
    
    # Get attendees email and name
    try:
        cursor.execute("SELECT first_name, last_name, email FROM attendee;")
        rows = cursor.fetchall()
        nbr_attendees = len(rows)

        # Loop through each attendee and send an email with a personalized subject
        for row in rows:
            email_address = str(row[2])
            first_name = str(row[0])
            personalized_subject = '{}: {}'.format(first_name, email_subject)
            email = Mail(
                from_email='maria.scharin@usa.net',
                to_emails='maria.scharin@usa.net',
                subject=personalized_subject,
                html_content=email_message)
            try:
                sg = SendGridAPIClient(settings.SENDGRID_API_KEY)
                response = sg.send(email)
                logging.info("Response status code: %s", response.status_code)
            except Exception as e:
                print(str(e))

    except (Exception, Error) as error:
        logging.info("Error while selecting data", error)
        if (conn):
            cursor.close()
            conn.close()
            logging.info("PostgreSQL connection is closed in select data block")

    # Update the notification table by setting the completed date and updating the status with the total number of attendees notified
    status_text = "Notified {} attendees".format(nbr_attendees)

    try:
        cursor.execute("UPDATE notification SET status = %s, completed_date = %s WHERE id = %s;", (status_text,str(notification_date),notification_id,))
        logging.info("Updated notification table with new status")

    except (Exception, Error) as error:
        logging.info("Error while updating notification table with new status", error)
        if (conn):
            cursor.close()
            conn.close()
            logging.info("PostgreSQL connection is closed in update notification block")
    
    # Clean up
    conn.commit()
    cursor.close()
    conn.close()