from pyspark.sql import SparkSession
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ParquetProcessingLogger")

def create_spark_session():
    """
    Initialize and return a Spark session with specific configurations.
    """
    spark = SparkSession.builder \
            .appName("NamkinProductionDwhStarSchema") \
            .config("spark.driver.host", "localhost") \
            .master("local[*]") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "50") \
            .config("spark.executor.cores", "4") \
            .getOrCreate()
    return spark

def convert_csv_to_parquet(input_path, output_path, delimiter=",", header=True, inferSchema=True, partitionBy=None):
    """
    Convert all CSV files in the specified input path to Parquet format with various options and save them to the output path.
    """
    if not os.path.exists(input_path):
        logger.error(f"Input path {input_path} does not exist.")
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    for file in os.listdir(input_path):
        if file.name.endswith(".csv"):
            try:
                file_path = os.path.join(input_path, file)
                df = spark.read.format("csv") \
                        .option("inferSchema", inferSchema) \
                        .option("header", header) \
                        .option("sep", delimiter) \
                        .load(file_path)
                output_file_path = os.path.join(output_path, file.name.replace('.csv', ''))
                if partitionBy:
                    df.write.mode('overwrite').partitionBy(partitionBy).parquet(output_file_path)
                else:
                    df.write.mode('overwrite').parquet(output_file_path)
                logger.info(f"Converted {file.name} to Parquet format successfully.")
            except Exception as e:
                logger.error(f"Failed to convert {file.name}: {e}")

def concatenate_parquet_files(input_path, output_file):
    """
    Read all Parquet files in the specified input path and concatenate them into a single DataFrame,
    then save it to the specified output file in Parquet format.
    """
    df = spark.read.format("parquet").load(input_path + "/*")
    df_single_partition = df.repartition(1)

    df_single_partition.write.mode('overwrite').format("parquet").save(output_file)
    logger.info(f"Concatenated parquet files into {output_file}")

if __name__ == "__main__":
    input_path = '../../data/machines'
    output_path = '../../data/machines_parquet/'
    final_output_path = '../../data/machines_all_parquet/'

    spark = create_spark_session()
    concatenate_parquet_files(input_path, output_path)
    concatenate_parquet_files(output_path, final_output_path)