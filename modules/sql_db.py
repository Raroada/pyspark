import typing
from pyspark.sql import SparkSession 

class Isql_conn:
    def __init__(self, user:str, password:str, server:str, database:str, sql_port:int, sql_driver:str):
        self.odbc_conn_url = self.create_odbc_url(user, password, server, database, sql_driver)
        self.jdbc_conn_url = self.create_jdbc_url(server, database, sql_port)
        self.properties.user = user
        self.properties.password = password
        self.properties.driver = sql_driver

    def create_odbc_url(user:str, password:str, server:str, database:str, sql_driver:str) -> str:
        if sql_driver is None:
            sql_driver = "ODBC Driver 17 for SQL Server"
        return f"Driver={sql_driver};Server={server}.database.windows.net;Database={database};UID={user};PWD={password};"

    def create_jdbc_url(server:str, database:str, sql_port:int) -> str:
        if sql_port:
            return f"jdbc:sqlserver://{server}.database.windows.net:{sql_port};database={database}"
        return f"jdbc:sqlserver://{server}.database.windows.net:1433;database={database}"

class sql_db:
    def __init__(self):
        self.creds = Isql_conn()
        pass

    def select(query: str):
        # Select from server based on the query provided
        None
    
    def insert():
        # assume append/insert
        None

    def delete():
        # assume delete
        None

    def upsert():
        # assume update then insert
        None

class sql_db_dataframe(Isql_conn):
    def __init__(self, user:str, password:str, server:str, database:str, sql_port:int, sql_driver:str):
        Isql_conn.__init__(self, user, password, server, database, sql_port, sql_driver)
        self.spark = SparkSession._getActiveSessionOrCreate()

    def select(self, query:str):
        return (
            self.spark.read
            .format("jdbc")
            .option(self.jdbc_conn_url)
            .option("driver", self.properties.driver)
            .option("query", query)
            .option("user", self.properties.user)
            .option("password", self.properties.password) 
            .load()
        )

