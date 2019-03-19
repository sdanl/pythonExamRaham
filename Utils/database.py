import json
from collections import defaultdict
from Utils.database_connection import DatabaseConnection
from dicttoxml import dicttoxml


def get_employees(db_path):
    with DatabaseConnection(db_path) as connection:
        cursor = connection.cursor()
        cursor.execute('SELECT * FROM employees')
        for row in cursor:
            print("row = " + str(row))


def get_invoice_count_by_country(db_path, country):
    with DatabaseConnection(db_path) as connection:
        cursor = connection.cursor()
        execution_str1 = "SELECT BillingCountry, count(*) as CNT FROM invoices "
        execution_str2 = "WHERE BillingCountry ='" + country + "' group by BillingCountry"
        cursor.execute(execution_str1 + execution_str2)
        for row in cursor:
            print("row = " + str(row))


def get_invoice_count_per_country_to_csv(db_path):
    count_per_country_file = 'OutputFiles\\count_per_country.csv'
    open(count_per_country_file, 'w').close()  # clear file before appending
    with DatabaseConnection(db_path) as connection:
        cursor = connection.cursor()
        # ----CREATE TABLE IF DOESNT EXIST
        cursor.execute("CREATE TABLE IF NOT EXISTS invoice_count_per_country(Country text primary key, Cnt integer)")
        cursor.execute("DELETE FROM invoice_count_per_country")
        connection.commit()
        # ---
        execution_str1 = "SELECT BillingCountry, count(*) as CNT FROM invoices "
        execution_str2 = "GROUP BY BillingCountry"
        cursor.execute(execution_str1 + execution_str2)
        result_set = []
        with open(count_per_country_file, 'a') as file:
            for row in cursor:
                list_row = list(row)
                # print(list_row[0] + ',' + str(list_row[1]))
                file.write(list_row[0] + ',' + str(list_row[1]) + '\n')
                result_set.append((list_row[0], list_row[1]))
        # ---- INSERT VALUES INTO TABLE
        cursor.executemany("INSERT INTO invoice_count_per_country VALUES (?, ? )", result_set)
        # ----


def get_albums_per_country_to_json(db_path):
    country_albums_defdict = defaultdict(list)  # if country doesnt yet exist it will be created
    albums_per_country_file = 'OutputFiles\\albums_per_country.json'
    open(albums_per_country_file, 'w').close()  # clear file before appending
    with DatabaseConnection(db_path) as connection:
        cursor = connection.cursor()
        execution_str1 = "SELECT DISTINCT C.BillingCountry, A.Title "
        execution_str2 = "FROM albums A JOIN tracks T ON A.AlbumId = T.AlbumId "
        execution_str3 = "JOIN invoice_items I ON T.TrackId = I.TrackId "
        execution_str4 = "JOIN invoices C on I.InvoiceLineId = C.InvoiceId "
        execution_str5 = "ORDER BY C.BillingCountry "
        cursor.execute(execution_str1 + execution_str2 + execution_str3 + execution_str4 + execution_str5)

        for row in cursor:
            list_row = list(row)
            # print(list_row[0] + ',' + str(list_row[1]))
            country_albums_defdict[list_row[0]].append(list_row[1])

    with open(albums_per_country_file, 'a') as file:
        # print(country_albums_defdict)
        json.dump(country_albums_defdict, file)


def get_albums_by_country_and_year_to_xml(db_path, country, year):  # ------------> continue
    albums_by_country_and_year_file = 'OutputFiles\\albums_by_country_and_year.xml'
    # open(albums_by_country_and_year_file, 'w').close()  # clear file before appending
    with DatabaseConnection(db_path) as connection:
        cursor = connection.cursor()
        # ----CREATE TABLE IF DOESNT EXIST
        cursor.execute("CREATE TABLE IF NOT EXISTS albums_by_country_and_year (Country text, Disk text, Year integer, SalesCount integer)")
        # cursor.execute("DELETE FROM invoice_count_per_country")
        connection.commit()
        # ---
        execution_str1 = "SELECT DISTINCT C.BillingCountry, A.Title, strftime('%Y', C.InvoiceDate) Year ,COUNT( C.InvoiceId) Cnt "
        execution_str2 = "FROM albums A JOIN tracks T ON A.AlbumId = T.AlbumId "
        execution_str3 = "JOIN invoice_items I ON T.TrackId = I.TrackId "
        execution_str4 = "JOIN invoices C on I.InvoiceLineId = C.InvoiceId "
        execution_str5 = "JOIN genres G ON T.GenreId = G.GenreId "
        execution_str6 = "WHERE G.Name ='Rock' AND C.BillingCountry = '" + country + "' AND strftime('%Y', C.InvoiceDate) >= '" + year + " "
        execution_str7 = "GROUP BY C.BillingCountry, A.Title, strftime('%Y', C.InvoiceDate) "
        execution_str8 = "ORDER BY Cnt DESC LIMIT 1"

        # print(execution_str1 + execution_str2 + execution_str3 + execution_str4 + execution_str5 + execution_str6 + execution_str7 + execution_str8)
        cursor.execute(execution_str1 + execution_str2 + execution_str3 + execution_str4 + execution_str5 + execution_str6 + execution_str7 + execution_str8)

        for row in cursor:
            list_row = list(row)
            # print(list_row[0] + ', ' + list_row[1] + ', ' + str(list_row[2]) + ', ' + str(list_row[3]))
            country_year_albums_dict = {'Country:' + list_row[0] + ','
                                        'Disk:' + list_row[1] + ','
                                        'Year:' + str(list_row[2]) + ','
                                        'Sales Count:' + str(list_row[3])
                                        }

        # print(country_year_albums_dict)
        xml = dicttoxml(country_year_albums_dict, custom_root='BestSellers', attr_type=False)

        with open(albums_by_country_and_year_file, 'w') as file:
            # print(xml)
            file.write(str(xml).strip())

        # ---- INSERT VALUES INTO TABLE
        cursor.execute("INSERT INTO albums_by_country_and_year VALUES (?, ?, ?, ?)", (list_row[0], list_row[1], list_row[2], list_row[3]))
        # ----
