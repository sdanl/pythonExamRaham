import pika
from Utils import database

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='msg')


def callback(ch, method, properties, body):
    print("Received %r" % body)
    message = str(body)
    db_path, country, year = message.split(',')
    db_path = db_path[2:]
    print('MESSAGE: ' + db_path + ' ' + country + ' ' + year)
    # database.get_employees(db_path)
    # database.get_invoice_count_by_country(db_path, country)
    database.get_invoice_count_per_country_to_csv(db_path)
    database.get_albums_per_country_to_json(db_path)
    database.get_albums_by_country_and_year_to_xml(db_path, country, year)


channel.basic_consume(callback,
                      queue='msg',
                      no_ack=True)

print('Waiting for messages. To exit press CTRL+C')
channel.start_consuming()


