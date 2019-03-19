import send


def menu():

    db_path = input("Please enter full DB path: \n")
    country = input("Please enter country name: \n")
    year = input("Please enter year (YYYY): \n")

    if db_path == '':
        db_path = 'C:\\sqlite\\chinook.db'
    if country == '':
        country = 'Brazil'
    if year == '':
        year = '2012'

    message = db_path + ',' + country + ',' + year
    send.send_message(message)


menu()
