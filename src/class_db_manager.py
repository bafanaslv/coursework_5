import psycopg2
from psycopg2 import Error
from src.class_abstract import DataBaseManager


class DBManager(DataBaseManager):
    def __init__(self, connection_parameters):
        self.database = connection_parameters["database"]
        self.user = connection_parameters["user"]
        self.password = connection_parameters["password"]
        self.host = connection_parameters["host"]
        self.port = connection_parameters["port"]
        self.connection = None
        self.cursor = None

    def create_database(self):
        try:
            # Подключение к существующей базе данных
            self.connection = psycopg2.connect(user=self.user,
                                               password=self.password,
                                               host=self.host,
                                               port=self.port)
            # Курсор для выполнения операций с базой данных
            self.cursor = self.connection.cursor()
            self.connection.autocommit = True

            self.cursor.execute(f"DROP DATABASE IF EXISTS {self.database}")
            self.cursor.execute(f"CREATE DATABASE {self.database}")
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if self.connection is not None:
                self.cursor.close()
                self.connection.close()
                print(f"База данных {self.database} создана. Соединение с PostgreSQL закрыто")

    def create_tables(self, selected_emp):
        try:
            # Подключение к созданной базе данных self.database
            self.connection = psycopg2.connect(user=self.user,
                                               password=self.password,
                                               host=self.host,
                                               port=self.port,
                                               database=self.database)
            # Курсор для выполнения операций с базой данных
            self.cursor = self.connection.cursor()
            self.connection.autocommit = True
            self.cursor.execute(f"DROP TABLE IF EXISTS employers")
            self.cursor.execute("CREATE TABLE employers (employer_id int PRIMARY KEY NOT NULL, "
                                "employer_name varchar(100) NOT NULL, alternate_url varchar(100) NOT NULL)")
            for data in selected_emp:
                print(data[1])
                print(data[2])
                data_ins = []
                data_ins.append(int(data[0]))
                data_ins.append(int(data[1]))
                data_ins.append(int(data[2]))
                print(data_ins)
                self.cursor.execute(
                    "INSERT INTO employers (employer_id, employer_name, alternate_url) VALUES (%s, %s, %s) returning *", data_ins)

        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if self.connection is not None:
                self.cursor.close()
                self.connection.close()
                print(f"Соединение с PostgreSQL закрыто")
