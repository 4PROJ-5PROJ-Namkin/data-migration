from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, explode, col, to_date, monotonically_increasing_id, regexp_replace, lit, date_format
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType, DateType, ShortType, LongType, IntegerType, TimestampType
import os
import logging
from py4j.protocol import Py4JJavaError
from dotenv import load_dotenv
from dwh_prototype_udf_utils import parse_date, string_to_int_list, convert_timestamp_to_date

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
        logging.error(f"Input path {input_path} does not exist.")
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    for file_name in os.listdir(input_path):
        if file_name.endswith(".csv"):
            try:
                logging.info(f"Starting to convert {file_name} CSV file to Parquet format.")
                
                file_path = os.path.join(input_path, file_name)
                df = spark.read.format("csv") \
                        .option("inferSchema", inferSchema) \
                        .option("header", header) \
                        .option("sep", delimiter) \
                        .load(file_path)
                
                output_file_path = os.path.join(output_path, file_name.replace('.csv', ''))
                if partitionBy:
                    df.write.mode('overwrite').partitionBy(partitionBy).parquet(output_file_path)
                else:
                    df.write.mode('overwrite').parquet(output_file_path)
                logging.info(f"Converted {file_name} to Parquet format successfully.")
            except Exception as e:
                logging.error(f"Failed to convert {file_name}: {e}")

def concatenate_parquet_files(input_path, output_file):
    """
    Read all Parquet files in the specified input path and concatenate them into a single DataFrame,
    then save it to the specified output file in Parquet format.
    """
    df = spark.read.format("parquet").load(input_path + "/*")
    df_single_partition = df.repartition(1)

    df_single_partition.write.mode('overwrite').format("parquet").save(output_file)
    logging.info(f"Concatenated parquet files into {output_file}")


def read_parquet_with_spark(file_path, file_name):
    """
    Reads a Parquet file into a Spark DataFrame.
    """
    try:
        logging.info(f"Starting to read the {file_name} Parquet file from {file_path}.")
        df = spark.read.format("parquet").load(file_path)
        logging.info(f"Successfully read the {file_name} Parquet file from {file_path} into a Spark DataFrame.")
        return df
    
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading the {file_name} Parquet file from {file_path}: {e}")
        return None

def read_excel_with_spark(file_path, file_name, sheet_name=None):
    """
    Reads an Excel file into a Spark DataFrame.
    """
    try:
        logging.info(f"Starting to read the {file_name} Excel file.")
        
        read_excel_query = spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("inferSchema", "true")
        
        if sheet_name:
            read_excel_query = read_excel_query.option("dataAddress", f"{sheet_name}!")
        
        df = read_excel_query.load(file_path)                
        logging.info(f"Successfully read the {file_name} Excel file into a Spark DataFrame.")
        return df

    except Py4JJavaError as e:
        if "java.util.NoSuchElementException: head of empty list" in str(e.java_exception):
            logging.warning(f"The {file_name} Excel file is empty. Skipping file processing.")
            return None
        else:
            logging.error(f"Error occurred while reading the {file_name} Excel file: {e}")
            return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading the {file_name} Excel file: {e}")
        return None

def populate_fact_supply_chain_table(fact_supply_chain_df, part_df, material_price_df, machine_df):
    """
    Transforms and populates the fact_supply_chain_table with data from multiple source DataFrames.
    Applies necessary transformations and joins to create a final DataFrame with the required schema.
    """
    try:
        logging.info("Starting to transform DataFrame for fact_supply_chain table.")
        convert_timestamp_to_date_udf = udf(convert_timestamp_to_date, TimestampType())
        fact_supply_chain_df = fact_supply_chain_df.withColumn('unique_id', monotonically_increasing_id()) \
                                                .withColumn("timeOfProduction", convert_timestamp_to_date_udf(col("timeOfProduction"))) \
                                                .withColumn("timeOfProduction", date_format("timeOfProduction", "yyyy-MM-dd HH:mm:ss.SSS"))

        fact_supply_chain_df = fact_supply_chain_df.alias('fact').join(
            part_df.select('id', 'materialId').alias('part_info'),
            col('fact.partId') == col('part_info.id'),
            'inner'
        )
        
        fact_supply_chain_df = fact_supply_chain_df.join(
            material_price_df.select('id', 'materialId').alias('material_price'),
            col('part_info.materialId') == col('material_price.materialId'),
            'inner'
        )

        fact_supply_chain_df = fact_supply_chain_df.join(
            machine_df.select('machineId').alias('machine'),
            col('fact.machineId') == col('machine.machineId'),
            'inner'
        )

        output_df = fact_supply_chain_df.select(
            col('fact.unique_id').alias('id'),
            col('fact.timeOfProduction'),
            col('fact.var5').alias('isDamaged'),
            col('fact.partId'),
            lit(None).alias('contractId'),
            col('part_info.materialId'),
            col('material_price.id').alias('materialPriceId'),
            col('machine.machineId').alias('machineId'),
        )

        logging.info("Successfully transformed and merged DataFrame for fact_supply_chain table.")
        return output_df

    except Exception as e:
        logging.error(f"An error occurred while transforming the DataFrame for fact_supply_chain table: {e}")
        return None

def populate_dim_material_price_table(material_df, price_col='prices'):
    """
    Transforms and enriches the material DataFrame for the dim_material_price table by exploding 
    the serialized JSON 'prices' column into individual rows with 'price' and 'date' columns.
    
    The 'd' field in the JSON is assumed to be a date string, which is transformed into a date object
    in the format specified by the date_format parameter. An auto-increment 'id' column is also added.
    """    
    try:
        logging.info("Starting to transform DataFrame for dim_material_prices table.")
        dim_schema = ArrayType(StructType([
            StructField("price", DoubleType()),
            StructField("d", StringType())
        ]))
        
        transformed_material_df = material_df.withColumn(price_col, from_json(col(price_col), dim_schema))
        transformed_material_df = transformed_material_df.withColumn("exploded", explode(col(price_col)))
        parse_date_udf = udf(parse_date, DateType())

        final_df = transformed_material_df.withColumn("id", monotonically_increasing_id()) \
                                         .withColumn("price", col("exploded.price")) \
                                         .withColumn("date", parse_date_udf(col("exploded.d"))) \
                                         .withColumn("materialId", col("id").cast(ShortType())) \
                                         .select("id", "price", "date", "materialId")
        
        logging.info("Successfully transformed DataFrame for dim_material_price table.")
        return final_df

    except Exception as e:
        logging.error("An error occurred while transforming the DataFrame for dim_material_price table: %s", e)

def populate_dim_part_information_table(part_information_df, material_col='meterials', machine_col='machine'):
    """
    Transforms and enriches the part information DataFrame by exploding the 'machine' and 'meterials' columns.
    """
    try:
        logging.info("Starting to transform DataFrame for dim_part_information table.")

        string_to_int_list_udf = udf(string_to_int_list, ArrayType(IntegerType()))
        part_information_df = part_information_df \
            .withColumn(material_col, regexp_replace(col(material_col), "'", "")) \
            .withColumn(material_col, string_to_int_list_udf(col(material_col))) \
            .withColumn(machine_col, string_to_int_list_udf(col(machine_col)))

        part_information_df = part_information_df \
            .withColumn(material_col, explode(col(material_col))) \
            .withColumn(machine_col, explode(col(machine_col))) \
            .withColumn('id', monotonically_increasing_id())
            
        final_df = part_information_df \
            .withColumnRenamed(machine_col, "machineId") \
            .withColumnRenamed(material_col, "materialId")
            
        logging.info("Successfully transformed DataFrame for dim_part_information table.")
        return final_df
    
    except Exception as e:
        logging.error(f"An error occurred while transforming the DataFrame for dim_part_information table: {e}")

def populate_dim_machine_table(part_information_df):
    """
    Transforms and enriches the machine DataFrame by extracting the unique material ids of the part information 
    transformed and leveraged DataFrame in order to ensure referential integrity in both sides.
    """
    try:
        logging.info("Starting to transform DataFrame for dim_machine table.")
        machine_df = part_information_df.select("machineId") \
                                        .dropDuplicates() \
                                        .orderBy('machineId', ascending=True)
        
        logging.info("Successfully transformed DataFrame for dim_machine table.")
        return machine_df
    except Exception as e:
        logging.error(f"An error occurred while transforming the DataFrame for dim_machine table: {e}")

def export_data_into_dwh_table(df, server, database, username, password, table_name):
    """
    Inserts records from a DataFrame into a DWH table persisted in a SQL Server database.
    """
    try:
        jdbc_url = (
            f"jdbc:sqlserver://{server}:1433;"
            f"databaseName={database};"
            f"user={username};"
            f"password={password};"
        )

        df.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", table_name) \
          .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
          .save()
        
        logging.info(f"Data inserted into the DWH {table_name} table successfully.")

    except Exception as e:
        logging.error(f"An error occurred while inserting {table_name} data into SQL Server: {e}")

if __name__ == "__main__":
    log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'dwh_populate_tables_star_schema.log'))
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
                    logging.FileHandler(log_file_path),
                    logging.StreamHandler()
                ]
        )
    
    load_dotenv('../../.env')
    server = os.getenv('DB_HOST')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')

    input_path = '../../data/machines'
    output_path = '../../data/machines_parquet/'
    final_output_path = '../../data/machines_parquet/machines_all_parquet/'

    spark = create_spark_session()
    convert_csv_to_parquet(input_path, output_path)
    concatenate_parquet_files(output_path, final_output_path)

    material_df = read_excel_with_spark("../../data/material-data.xlsx" , "Material")
    part_information_df = read_excel_with_spark("../../data/part-reference.xlsx", "Part Information")
    sales_df = read_excel_with_spark("../../data/sales.xlsx", "Sales")
    supply_chain_df = read_parquet_with_spark(final_output_path, 'Supply Chain')
    
    material_price_df = populate_dim_material_price_table(material_df)
    part_df = populate_dim_part_information_table(part_information_df)
    machine_df = populate_dim_machine_table(part_df)
    supply_chain_df = populate_fact_supply_chain_table(supply_chain_df, part_df, material_price_df, machine_df)

    material_df = material_df.withColumn('id', col('id').cast(ShortType())) \
                            .select('id', 'name')
    part_information_df = part_information_df.withColumn('id', col('id').cast(ShortType())) \
                                            .select('id', 'defaultPrice', 'timeToProduce')

    try: 
        target_df = [material_df, material_price_df, part_information_df, machine_df, supply_chain_df]
        target_tables = ['dim_material', 'dim_material_prices', 'dim_part_information', 'dim_machine', 'fact_supply_chain']

        for t_df, t_name in zip(target_df, target_tables):
            export_data_into_dwh_table(t_df, server, database, username, password, t_name)
    except Exception as e:
        logging.error(f'Failed to serialize the values of the {t_df} DataFrame in the {t_name} table: {e}')
