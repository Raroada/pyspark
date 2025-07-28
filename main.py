from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType,StringType

spark = (
    SparkSession.builder
        .master("local")
        .appName("Local build")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
)

df = spark.createDataFrame(
    [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])

df.show()

_jdbc_url = ''
_jdbc_conn = {}

class jdbc_connection:
    def __init__(
        self, spark, jdbc_hostname, jdbc_port, database, username, password
    ):
        self._jdbc_url = "jdbc:sqlserver://{0}:{1}/{2}".format(jdbc_hostname, jdbc_port, database)
        self._jdbc_conn_ = {
            "user": username,
            "password": password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }
        self.spark = spark

    def get(self, query:str = None, sql_table:str = None, num_partition:int = 4):
        query_str = query if query is not None else table

        options = {
            "driver":self._jdbc_conn_.get("driver")
            ,"user":self._jdbc_conn_.get("username")
            ,"password":self._jdbc_conn_.get("password")
        }

        if

        return (
            self.spark.read
            .format("jdbc")
            .option("driver", self._jdbc_conn['driver'])
            .option("url", _jdbc_url)
            .option("dbtable", query_str)
            .option("user", _jdbc_conn['user'])
            .option("password", _jdbc_conn['password'])
            .option("numPartitions", num_partition)
            .load()
        )
        

    
    


def get_data():
    return None

def process_data():
    return None

def send_data():
    return None