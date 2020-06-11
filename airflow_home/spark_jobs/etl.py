import configparser
import os
import sys

from pyspark.sql import SparkSession


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_automobile_data(spark, input_data, output_data):
    automobile_data = input_data + "automobile-data.csv"
    df = spark.read.csv(automobile_data, header=True, inferSchema=True)

    automobile_table = df.select('num-of-cylinders', 'engine-size').distinct()

    automobile_table.write.mode('overwrite').parquet(output_data + 'automobile')


def check_results(spark, output_data):
    df = spark.read.parquet(output_data + 'automobile')
    df.select('num-of-cylinders', 'engine-size')


def main():
    if len(sys.argv) == 3:
        # aws cluster mode - Here we could read an offset Date passed by the Airflow Runtime
        input_data = sys.argv[1]
        output_data = sys.argv[2]
    else:
        # local mode
        config = configparser.ConfigParser()
        config.read('./dl.cfg')

        os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

        input_data = config['DATALAKE']['INPUT_DATA']
        output_data = config['DATALAKE']['OUTPUT_DATA']

    spark = create_spark_session()

    process_automobile_data(spark, input_data, output_data)
    check_results(spark, output_data)


if __name__ == "__main__":
    main()
