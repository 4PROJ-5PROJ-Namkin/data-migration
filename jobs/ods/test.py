import pandas as pd
import os
import numpy as np
import uuid
import pyarrow.parquet as pq
from ods_structure_tables_star_schema import DataWarehouseManager
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging
import random
import ast
from sqlalchemy import create_engine

log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'dwh_populate_tables_star_schema.log'))
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
                logging.FileHandler(log_file_path),
                logging.StreamHandler()
            ]
    )

def convert_csv_to_parquet(input_path, output_path, delimiter=","):
    if not os.path.exists(input_path):
        logging.error(f"Input path {input_path} does not exist.")
        return
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    for file_name in os.listdir(input_path):
        if file_name.endswith(".csv"):
            try:
                logging.info(f"Starting to convert {file_name} CSV file to Parquet format.")
                file_path = os.path.join(input_path, file_name)
                df = pd.read_csv(file_path, delimiter=delimiter)
                parquet_file = file_name.replace('.csv', '.parquet')
                output_file_path = os.path.join(output_path, parquet_file)
                df.to_parquet(output_file_path)
                logging.info(f"Converted {file_name} to Parquet format successfully.")
            except Exception as e:
                logging.error(f"Failed to convert {file_name}: {e}")

def concatenate_parquet_files(input_path, output_file):
    dfs = []
    for file_name in os.listdir(input_path):
        if file_name.endswith(".parquet"):
            file_path = os.path.join(input_path, file_name)
            dfs.append(pd.read_parquet(file_path))
    
    df_concatenated = pd.concat(dfs)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df_concatenated.to_parquet(output_file)
    logging.info(f"Concatenated parquet files into {output_file}")

def read_parquet_with_pandas(file_path, file_name):
    """
    Reads a Parquet file into a Pandas DataFrame.
    
    Args:
    file_path (str): The complete file path to the Parquet file.
    file_name (str): The name of the file to be logged.
    
    Returns:
    pandas.DataFrame: A DataFrame if the file is successfully read, None otherwise.
    """
    try:
        logging.info(f"Starting to read the {file_name} Parquet file from {file_path}.")
        df = pd.read_parquet(file_path)
        logging.info(f"Successfully read the {file_name} Parquet file from {file_path} into DataFrame.")
        return df
    
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading the {file_name} Parquet file from {file_path}: {e}")
        return None

def read_excel_with_pandas(file_path, file_name, sheet_name=None):
    """
    Reads an Excel file into a pandas DataFrame.
    """
    try:
        logging.info(f"Starting to read the {file_name} Excel file.")
        
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        
        if df.empty:
            logging.warning(f"The {file_name} Excel file is empty. Skipping file processing.")
            return None

        logging.info(f"Successfully read the {file_name} Excel file into a DataFrame.")
        return df

    except Exception as e:
        logging.error(f"Error occurred while reading the {file_name} Excel file: {e}")

def parse_date_pandas(date_str):
    """
    Parses a date string into the correct date format.
    """
    try:
        return datetime.datetime.strptime(date_str, '%d-%m-%Y').date()
    except ValueError as ve:
        logging.error(f"Date parsing error: {ve}")
        return None

def populate_dim_material_price_table_pandas(material_df, price_col='prices', date_format='%m-%d-%Y'):
    try:
        logging.info("Transforming DataFrame for dim_material_prices table.")

        material_df[price_col] = material_df[price_col].apply(eval)
        material_df = material_df.explode(price_col)
        material_df['price'] = material_df[price_col].apply(lambda x: x.get('price'))
        material_df['date'] = material_df[price_col].apply(lambda x: parse_date_pandas(x.get('d')))

        if pd.api.types.is_datetime64_any_dtype(material_df['date']):
            material_df['date'] = material_df['date'].dt.strftime(date_format)
        else:
            logging.error("Date column is not recognized as datetime.")

        # Copie de l'identifiant original (si c'est ce que vous souhaitez)
        material_df['materialId'] = material_df['id']

        # Génération d'un nouvel identifiant unique et monolithique pour chaque ligne
        material_df['id'] = range(1, 1 + len(material_df))

        logging.info("Successfully transformed DataFrame for dim_material_price table.")
        return material_df.drop(columns=[price_col])

    except Exception as e:
        logging.error(f"An error occurred while transforming the DataFrame: {e}")
        return None


def string_to_int_list(string_list):
    """
    Converts a string representation of a list into an actual list of integers.
    """
    try:
        return ast.literal_eval(string_list)
    except ValueError:
        return []

def populate_dim_part_information_table_pandas(part_information_df, material_col='meterials', machine_col='machine'):
    """
    Transforms and enriches the part information DataFrame by exploding the 'machine' and 'meterials' columns.
    """
    try:
        logging.info("Starting to transform DataFrame for dim_part_information table.")

        # Convert string representations of lists into actual lists
        part_information_df[material_col] = part_information_df[material_col].apply(string_to_int_list)
        part_information_df[machine_col] = part_information_df[machine_col].apply(string_to_int_list)

        # Explode the 'meterials' and 'machine' columns into separate rows
        part_information_df = part_information_df.explode(material_col)
        part_information_df = part_information_df.explode(machine_col)

        # Rename the exploded columns to match the expected output
        part_information_df = part_information_df.rename(columns={material_col: 'materialId', machine_col: 'machineId'})
        #part_information_df.id = range(1, len(part_information_df)+1)
        logging.info("Successfully transformed DataFrame for dim_part_information table.")
        return part_information_df

    except Exception as e:
        logging.error(f"An error occurred while transforming the DataFrame for dim_part_information table: {e}")

def populate_dim_machine_table_pandas(part_information_df):
    try:
        logging.info("Starting to transform DataFrame for dim_machine table.")
        machine_df = part_information_df.machineId.drop_duplicates()
        logging.info("Successfully transformed DataFrame for dim_machine table.")

        return machine_df
    except Exception as e:
        logging.error(f"An error occurred while transforming the DataFrame for dim_machine table: {e}")       




def populate_fact_supply_chain_table_pandas(fact_supply_chain_df, part_df, material_price_df, machine_df):
    try:
        logging.info("Starting to transform DataFrame for fact_supply_chain table.")

        # Convertir les colonnes de jointure en entiers
        fact_supply_chain_df['partId'] = fact_supply_chain_df['partId'].astype('Int64')  # Utilisation de 'Int64' pour les entiers pandas nullable
        fact_supply_chain_df['machineId'] = fact_supply_chain_df['machineId'].astype('Int64')
        
        part_df['id'] = part_df['id'].astype('Int64')
        part_df['materialId'] = part_df['materialId'].astype('Int64')
        
        material_price_df['materialId'] = material_price_df['materialId'].astype('Int64')
        
        machine_df = machine_df.to_frame(name='machineId')
        machine_df['machineId'] = machine_df['machineId'].astype('Int64')
        
        # Effectuer les fusions
        merged_df = pd.merge(fact_supply_chain_df, part_df[['id', 'materialId']], left_on='partId', right_on='id', how='inner')
        merged_df = pd.merge(merged_df, material_price_df, on='materialId', how='inner')
        merged_df = pd.merge(merged_df, machine_df, on='machineId', how='inner')

        # Convertir isDamaged en type booléen
        merged_df['var5'] = merged_df['var5'].astype('bool')

        # Sélectionner et renommer les colonnes
        output_df = merged_df[['timeOfProduction', 'var5', 'partId', 'materialId', 'id', 'machineId']].copy()
        output_df.rename(columns={'var5': 'isDamaged', 'id': 'materialPriceId'}, inplace=True)
        
        # Convertir la colonne timeOfProduction en datetime
        output_df['timeOfProduction'] = pd.to_datetime(output_df['timeOfProduction'])
        
        # Ajouter la colonne contractId avec des valeurs nulles
        output_df['contractId'] = pd.NA
        
        # Créer une colonne id unique pour l'insertion dans la base de données
        output_df.insert(0, 'id', range(1, len(output_df) + 1))

        logging.info("Successfully transformed and merged DataFrame for fact_supply_chain table.")
        return output_df

    except Exception as e:
        logging.error(f"An error occurred while transforming the DataFrame for fact_supply_chain table: {e}")
        return None

def generate_random_date(year):
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    delta = (end_date - start_date).days
    random_day = random.randint(0, delta)
    return start_date + timedelta(days=random_day)

def convert_timestamp_to_date(timestamp):
    return pd.to_datetime(timestamp).date()

def populate_dim_sales_table(supply_chain_df, part_df):
    """
    Transforms and prepares a DataFrame for the dim_sales table. This function performs
    several operations including counting quantities of parts, calculating total cash,
    determining the maximum production year, and generating formatted client names.
    """
    try:
        logging.info("Transforming DataFrame for dim_sales table.")
        
        # Creating a unique identifier 'contractNumber' equivalent to 'order'
        supply_chain_df['contractNumber'] = supply_chain_df['order']

        # Calculating quantity
        quantity_df = supply_chain_df.groupby(['contractNumber', 'partId']).size().reset_index(name='quantity')
        
        # Calculating cost
        cost_df = quantity_df.merge(part_df, left_on='partId', right_on='id')
        cost_df['cash'] = cost_df['defaultPrice'] * cost_df['quantity']
        cost_df = cost_df.groupby('contractNumber')['cash'].sum().reset_index()

        # Determining the maximum production year
        supply_chain_df['timeOfProduction'] = supply_chain_df['timeOfProduction'].apply(convert_timestamp_to_date)
        supply_chain_df['year'] = supply_chain_df['timeOfProduction'].apply(lambda x: x.year)
        max_year_df = supply_chain_df.groupby('contractNumber')['year'].max().reset_index()
        
        # Merging DataFrames
        sales_df = cost_df.merge(max_year_df, on='contractNumber')
        
        # Generating random date and formatted client name
        sales_df['date'] = sales_df['year'].apply(generate_random_date)
        sales_df['clientName'] = 'CLIENT NO_' + sales_df['contractNumber'].astype(str)
        
        # Selecting final columns
        final_sales_df = sales_df[['contractNumber', 'clientName', 'cash', 'date']]

        logging.info("Successfully transformed DataFrame for dim_sales table.")
        return final_sales_df

    except Exception as e:
        logging.error(f"An error occurred while transforming the DataFrame for dim_sales table: {e}")
        raise e

def export_data_into_dwh_table_pandas(df, server, database, username, password, table_name):
    """
    Inserts records from a pandas DataFrame into a DWH table persisted in a SQL Server database.
    """
    try:
        connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&charset=utf8"

        engine = create_engine(connection_string)
        df.to_sql(table_name, engine, index=False, if_exists='append')
        
        logging.info(f"Data inserted into the DWH {table_name} table successfully.")

    except Exception as e:
        logging.error(f"An error occurred while inserting {table_name} data into SQL Server: {e}")


load_dotenv('../../.env')
server = os.getenv('DB_HOST')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')

if __name__ == "__main__":
    #material_df = read_excel_with_pandas(r"../../data/material-data.xlsx" , "Material", sheet_name="Sheet1")
    #print(material_df)
    #material_price_df = populate_dim_material_price_table_pandas(material_df)
    #print(material_price_df.dtypes)
    #part_information_df = read_excel_with_pandas(r"../../data/part-reference.xlsx" , "Part Information", sheet_name="Sheet1")
    #part_df = populate_dim_part_information_table_pandas(part_information_df)
    #print(part_df.dtypes)
    #print(material_price_df)
    #sales_df = read_excel_with_pandas(r"../../data/sales.xlsx" , "Sales", sheet_name="Sheet1")
    #machine_df = populate_dim_machine_table_pandas(part_df)
    #print(machine_df.dtypes)
    #export_data_into_dwh_table_pandas(part_information_df_transformed, server, database, username, password, 'fake_dim_part_information')
    #input_path = '../../data/machines'
    #output_path = '../../data/machines_parquet/'
    #final_output_path = '../../data/machines_parquet/machine_all_parquet'
    #supply_chain_df = read_parquet_with_pandas(final_output_path, 'Supply Chain')
    #supply_chain_df.machineId = supply_chain_df.machineId.astype(object)
    #sales_df = pd.read_csv('../../data/machines_parquet/contract.csv').sort_values(by=['contractNumber'])
    #print(sales_df.dtypes)
    #export_data_into_dwh_table_pandas(sales_df, server, database, username, password, 'dim_sales')    
    #supply_chain_df = populate_fact_supply_chain_table_pandas(supply_chain_df, part_df, material_price_df, machine_df)
    #convert_csv_to_parquet(input_path, output_path)
    #concatenate_parquet_files(output_path, final_output_path)
    #chunk_size = 10000  # Taille de chaque morceau
 #   chunk = pd.read_csv('../../data/machines_parquet/export (1).csv', chunksize=chunk_size)
    #df = pd.read_csv('../../data/machines_parquet/unit.csv')
    #df.to_parquet('../../data/machines_parquet/unit.parquet')
    unit_df = pd.read_parquet('../../data/machines_parquet/unit.parquet')
    #export_data_into_dwh_table_pandas(unit_df, server, database, username, password, 'dim_unit')
    #df_parquet = pd.read_csv('../../data/machines_parquet/fact_supply_chain.csv')
    #df_parquet.to_parquet("../../data/machines_parquet/fact_supply_chain.parquet")
    #df_parquet = pd.read_parquet("../../data/machines_parquet/fact_supply_chain.parquet")
    #df_sans_doublons = df_parquet.drop_duplicates()
    #print(df_sans_doublons.shape)
    #print(df_parquet.shape)
    #print(df_parquet.shape)
    #df_parquet['id'] = [uuid.uuid4() for _ in range(len(df_parquet))]
    #print(df_parquet)
    #df_parquet = pd.read_parquet("../../data/machines_parquet/export.parquet")
    #df_parquet['machineId'] = df_parquet['machineId'].astype('Int64')
    #df_parquet['partId'] = df_parquet['partId'].astype('Int64')
    #df_parquet['materialId'] = df_parquet['materialId'].astype('Int64')
    #df_parquet['materialPriceId'] = df_parquet['materialPriceId'].astype('Int64')
    #df_parquet['contractId'] = df_parquet['contractId'].astype('Int64')
    #df_parquet['contractId'] = df_parquet['contractId'].fillna(0)
    #df_parquet['contractId'] = np.where(df_parquet['contractId'] == 0, (np.arange(len(df_parquet)) % 10) + 1, df_parquet['contractId'])
    #df_parquet['machineId'].replace(-1, np.nan)
    #print(df_parquet.dtypes)
    db_manager = DataWarehouseManager(server, database, username, password)
    db_manager.connect()
    #print(df_parquet.contractId.unique().tolist())
    for index, row in unit_df.iterrows():
        query = "INSERT INTO dim_unit (id, timeOfProduction) VALUES (?, ?)"
        params = (row['id'], row['timeOfProduction'])
        db_manager.execute_query(query, params)
     #   query = "INSERT INTO fact_supply_chain (machineId, partId, materialId, materialPriceId, materialPrice, partDefaultPrice, timeOfProduction, isDamaged) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
     #   params = (row['machineId'], row['partId'], row['materialId'], row['materialPriceId'], row['materialPrice'], row['partDefaultPrice'], row['timeOfProduction'], row['isDamaged'])
     #   db_manager.execute_query(query, params)
        #export_data_into_dwh_table_pandas(df_parquet.head(1), server, database, username, password, 'fact_supply_chain')
        #for chunk in chunks:
            # Traiter chaque morceau ici
        #   print(chunk.head())  # Exemple de traitement