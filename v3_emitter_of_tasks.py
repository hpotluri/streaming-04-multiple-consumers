"""
    This program sends a message to a queue on the RabbitMQ server.
    Make tasks harder/longer-running by adding dots at the end of the message.

    Author: Denise Case
    Date: January 15, 2023

"""

import string
import pika
import sys
import webbrowser
import csv

from util_logger import setup_logger


logging, logname = setup_logger(__file__)

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    try:
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()


def readData(filename):
    """
    Reads the file and creates a list of messages that is returned. 

    Parameters:
        filename (str): name of the csv file used to pull tasks from. 
    """

    listofTasks = []
    with open(filename, 'r') as csvFile: #Opeing the file 
        file = csv.reader(csvFile,delimiter='\n') #Reads the whole file 
        for row in file: #Looping through the file and adding each line as a string to the list. 
            print(row[0])
            listofTasks.append((row[0]))
    return listofTasks



# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  
    show_offer = False
    if show_offer:
        offer_rabbitmq_admin_site()


    #This is how i authmate my messages
    messageList = readData("tasks.csv")

    for message in messageList:
    # send the message to the queue
        send_message("localhost","task_queue2",message)