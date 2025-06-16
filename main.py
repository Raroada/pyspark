from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType,StringType

spark = (
    SparkSession.builder
        .master("local")
        .appName("Local build")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
)

schema = StructType(
            [
                StructField('random_column', StringType())
            ]
        )

df = spark.createDataFrame(
    [(14, "Tom"), (23, "Alice"), (16, "Bob")], ["age", "name"])

df.show()