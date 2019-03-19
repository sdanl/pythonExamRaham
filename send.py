import pika


def send_message(message):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    #message = 'C:\\sqlite\\chinook.db, ISRAEL, 2019'

    channel.queue_declare(queue='msg')

    channel.basic_publish(exchange='',
                          routing_key='msg',
                          body=message)
    print('Sent ' + message)
    connection.close()
