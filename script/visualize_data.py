from pyspark.sql.functions import col, row_number
from pyspark.sql.types import StructType,StructField, StringType, DoubleType, IntegerType
from pyspark.sql import Window
from pyspark.sql import SparkSession
import argparse

spark = (SparkSession
    .builder
    .appName("visualize_data")
    .getOrCreate())


def load_file(path):
    schema = StructType([
        StructField("attrs", StringType(), True),
        StructField("avg_rating", DoubleType(), True),
        StructField("num_review", IntegerType(), True),
        StructField("num_sold", IntegerType(), True),
        StructField("price", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("shipping", StringType(), True),
        StructField("url", StringType(), True),
        StructField("country", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("stock", StringType(), True),
        StructField("origin", StringType(), True),
        StructField("first_category", StringType(), True),
        StructField("second_category", StringType(), True),
        StructField("third_category", StringType(), True),
        StructField("description", StringType(), True),
        StructField("shop_name", StringType(), True),
        StructField("shop_like_tier", IntegerType(), True),
        StructField("shop_num_review", IntegerType(), True),
        StructField("shop_reply_percentage", DoubleType(), True),
        StructField("shop_reply_time", StringType(), True),
        StructField("shop_creation_time", IntegerType(), True),
        StructField("shop_num_follower", IntegerType(), True)
    ])

    df = spark.read.format("csv").schema(schema).load(path)

    return df

def grouping(df):
    w = Window.partitionBy('product_name').orderBy(col("price").desc())
    df = df.withColumn("row",row_number().over(w)).filter(col("row") == 1).drop("row")
    return df

def write_file(df, destination):

    (df  
        .coalesce(1)
        .write.option("header", True)
        .format("csv")
        .mode('overwrite')
        .csv(destination))

    print("Succeed!")

    return df

def get_visualize_data(path, destination):
    df = load_file(path)
    df = grouping(df)
    write_file(df, destination)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Get visualize data')

    parser.add_argument('--origin', 
                        type=str,
                        help='Read location')

    parser.add_argument('--destination', 
                        type=str,
                        help='Save location')

    args = parser.parse_args()

    get_visualize_data(args.origin, args.destination)