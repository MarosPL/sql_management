# use 'pip install pyodbc' command in your console before running the script
# use 'pip install pandas' command in your console before running the script
# install ODBC Driver 18 for SQL Server from https://go.microsoft.com/fwlink/?linkid=2202930
# ask for firewall rule

import pandas as pd
import pyodbc

# class sql_managment:


def main():
    # STATE HERE YOUR DATABASE SERVER
    server_name = 'hurtownia-zabawek.database.windows.net,1433'

    # STATE HERE YOUR DATABASE NAME
    # database_name = 'master'
    database_name = 'testDataBase'

    # YOUR CREDENTIALS - TO BE DONE IN TKINTER
    user_name = 'MarekKwiatkowski'
    user_password = 'Karton123'

    # START CONNECTION
    connection = create_server_connection(
        server_name, database_name, user_name, user_password)

    # READ DATA USING PANDAS
    # sql_query = 'SELECT * FROM TestTable'
    # data = execute_query_in_pandas(connection, sql_query)
    # print(data)

    # READ DATA USING CURSOR
    # sql_query = 'SELECT * FROM TestTable'
    # data2 = execute_query_from_cursor(connection, sql_query)
    # print(data2)

    # INSERT DATA TO TABLE
    # records = pd.DataFrame(
    # [('Czarek', 'S'), ('Darek', 'O'), ('Arek', 'A')], columns=['Name', 'Surname'])
    # records = [('Czarek', 'S'), ('Darek', 'O'), ('Arek', 'A')]
    # columns = 'Name,Surname'
    # insert_data_to_table(connection, 'TestTable', columns, records)

    # CREATE USER
    # connection = create_user(connection, 'USER_FROM_PYTHON', 'Karton123', 'hurtownia-zabawek.database.windows.net,1433', 'testDataBase', user_name, user_password)

    # GRANT USER PERMISSION
    # grant_user_permission(connection,'USER_FROM_PYTHON')

    # DROP USER
    # connection = drop_user(connection, 'USER_FROM_PYTHON', 'hurtownia-zabawek.database.windows.net,1433', 'TestDataBase', user_name, user_password)

    # DELETE RECORDS FROM TABLE
    # id_records = pd.DataFrame(['4', '5', '6'], columns=['ID'])
    # delete_data_from_table(connection, 'TestTable', id_records)

    # DROP TABLE
    # drop_table(connection, 'TestTable')

    # CREATE TEST TABLE
    # create_test_table(connection)

    # data = execute_query_in_pandas(connection, sql_query)
    # print(data)

    # CLOSE CONNECTION
    close_server_connection(connection)


def create_server_connection(server_name, database_name, user_name, user_password):
    try:
        connection = None
        conn_str = (
            r'DRIVER={ODBC Driver 18 for SQL Server};'
            r'SERVER=' + server_name + ';'
            r'DATABASE=' + database_name + ';'
            r'UID=' + user_name + ';'
            r'PWD=' + user_password + ';')

        connection = pyodbc.connect(conn_str)
        print('Azure Sql Database ' +
              database_name + ' connection successful')
    except Exception as e:
        print(e)
        print('Connection unsuccessful')
    return connection


def drop_table(connection, table_name):
    try:
        cursor = connection.cursor()
        sql_query = 'DROP TABLE ' + table_name
        cursor.execute(sql_query)
        cursor.commit()
        print('Table ' + table_name + ' has been successfuly droped')
    except Exception as e:
        print(e)
        print('Error during droping table ' + table_name)


def delete_data_from_table(connection, table_name, id_records):
    try:
        cursor = connection.cursor()
        sql_query = 'DELETE FROM ' + table_name + ' WHERE id = (?)'
        for index, row in id_records.iterrows():
            cursor.execute(sql_query, row.ID)
        cursor.commit()
        print('Data has been deleted from table ' + table_name)
    except Exception as e:
        print(e)
        print('Error during deleting the data from table ' + table_name)


def insert_data_to_table(connection, table_name, columns, records):
    try:
        if len(records[0]) > 1:
            values_count = ''
            for i in range(len(records[0])):
                values_count = values_count + ',?'
            values_count = values_count[1:]
        else:
            values_count = '?'
        cursor = connection.cursor()
        sql_query = 'INSERT INTO ' + table_name + \
            '(' + columns + ') VALUES (' + values_count + ')'
        print(sql_query)
        cursor.executemany(sql_query, records)
        cursor.commit()
        print('Data has been inserted to table ' + table_name)
    except Exception as e:
        print(e)
        print('Error during inserting data to the table ' + table_name)


def execute_query_from_cursor(connection, sql_query):
    try:
        cursor = connection.cursor()
        cursor.execute(sql_query)
        return cursor.fetchall()
    except Exception as e:
        print(e)
        print('Error during executing query')
        print('SQL Query: ' + sql_query)


def execute_query_in_pandas(connection, sql_query):
    try:
        return pd.read_sql_query(sql_query, connection)
    except Exception as e:
        print(e)
        print('Error during executing query')
        print('SQL Query: ' + sql_query)


def create_test_table(connection):
    try:
        cursor = connection.cursor()
        sql_query = (
            r'CREATE TABLE [dbo].[TestTable]'
            r'([ID] [int] IDENTITY(1,1) NOT NULL,'
            r'[Name] [nvarchar](max) NOT NULL,'
            r'[Surname] [nvarchar](max) NOT NULL,'
            r'CONSTRAINT [PK_TestTable] PRIMARY KEY CLUSTERED ([ID] ASC))')
        cursor.execute(sql_query)
        cursor.commit()
        print('TestTable has been created')
    except Exception as e:
        print(e)
        print('Error while creating TestTable')


def create_user(connection, user_name, user_password, server_name, database_name, admin_user, admin_password):
    try:

        cursor = connection.cursor()
        dbname = cursor.execute('SELECT DB_NAME()').fetchall()
        dbnamestring = str(dbname)
        dbname = dbnamestring[3:str.rindex(dbnamestring, '\'')]
        if dbname == 'master':
            sql_query = 'CREATE LOGIN ' + user_name + ' WITH PASSWORD = \'' + user_password + '\'; CREATE USER ' + \
                user_name + ' FOR LOGIN ' + user_name + \
                ' WITH DEFAULT_SCHEMA = dbo; '
            cursor.execute(sql_query)
            cursor.commit()
            print('User: ' + user_name + ' created in master')
            close_server_connection(connection)
            connection = create_server_connection(
                server_name, database_name, admin_user, admin_password)
            cursor = connection.cursor()
            sql_query = 'CREATE USER ' + user_name + ' FOR LOGIN ' + \
                user_name + ' WITH DEFAULT_SCHEMA = ' + database_name + '; '
            cursor.execute(sql_query)
            cursor.commit()
            print('User: ' + user_name + ' created in ' + database_name)
        else:
            close_server_connection(connection)
            connection = create_server_connection(
                server_name, 'master', admin_user, admin_password)
            cursor = connection.cursor()
            sql_query = 'CREATE LOGIN ' + user_name + ' WITH PASSWORD = \'' + user_password + '\'; CREATE USER ' + \
                user_name + ' FOR LOGIN ' + user_name + \
                ' WITH DEFAULT_SCHEMA = dbo; '
            cursor.execute(sql_query)
            cursor.commit()
            print('User: ' + user_name + ' created in master')
            close_server_connection(connection)
            connection = create_server_connection(
                server_name, database_name, admin_user, admin_password)
            cursor = connection.cursor()
            sql_query = 'CREATE USER ' + user_name + ' FOR LOGIN ' + \
                user_name + ' WITH DEFAULT_SCHEMA = ' + database_name + '; '
            cursor.execute(sql_query)
            cursor.commit()
            print('User: ' + user_name + ' created in ' + database_name)
        return connection
    except Exception as e:
        print(e)


def grant_user_permission(connection, user_name):
    try:
        cursor = connection.cursor()
        sql_query = 'ALTER ROLE db_datareader ADD MEMBER ' + user_name + \
            '; ALTER ROLE db_datawriter ADD MEMBER ' + user_name + '; '
        cursor.execute(sql_query)
        cursor.commit()
        print('Permission for granted for user ' + user_name)
    except Exception as e:
        print(e)
        print('Error while granting permission to user ' + user_name)


def drop_user(connection, user_name, server_name, database_name, admin_user, admin_password):
    try:
        cursor = connection.cursor()
        dbname = cursor.execute('SELECT DB_NAME()').fetchall()
        dbnamestring = str(dbname)
        dbname = dbnamestring[3:str.rindex(dbnamestring, '\'')]
        if dbname == 'master':
            sql_query = 'DROP LOGIN ' + user_name + ';DROP USER ' + user_name + '; '
            cursor.execute(sql_query)
            cursor.commit()
            close_server_connection(connection)
            connection = create_server_connection(
                server_name, database_name, admin_user, admin_password)
            cursor = connection.cursor()
            sql_query = 'DROP USER ' + user_name
            cursor.execute(sql_query)
            cursor.commit()
        else:
            sql_query = 'DROP USER ' + user_name
            cursor.execute(sql_query)
            cursor.commit()
            close_server_connection(connection)
            connection = create_server_connection(
                server_name, 'master', admin_user, admin_password)
            sql_query = 'DROP USER ' + user_name + '; DROP LOGIN ' + user_name
            cursor.execute(sql_query)
            cursor.commit()
        print('User ' + user_name + ' has been successfuly deleted')
        return connection
    except Exception as e:
        print(e)
        print('Error during deleting user ' + user_name)


def close_server_connection(connection):
    try:
        if not connection == None:
            connection.close()
    except Exception as e:
        print(e)

# main()
