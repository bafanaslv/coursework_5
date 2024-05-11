import psycopg2
from psycopg2 import Error


class DBManager():
    def __init__(self, connection_parameters):
        self.user = connection_parameters["user"]
        self.password = connection_parameters["password"]
        self.host = connection_parameters["host"]
        self.port = connection_parameters["port"]
        self.connection = None
        self.cursor = None


    def create_database(self, database_name):
        try:
            # Подключение к существующей базе данных
            self.connection = psycopg2.connect(user=self.user,
                                               password=self.password,
                                               host=self.host,
                                               port=self.port)
            # Курсор для выполнения операций с базой данных
            self.cursor = self.connection.cursor()
            self.connection.autocommit = False

            self.cursor.execute(f'DROP DATABASE IF EXISTS {database_name}')
            self.cursor.execute(f'CREATE DATABASE {database_name}')
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if self.connection is True:
                self.cursor.close()
                self.connection.close()
                print("Соединение с PostgreSQL закрыто")
