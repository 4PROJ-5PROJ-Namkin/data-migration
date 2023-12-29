import pyodbc
import logging
from dotenv import load_dotenv
import os

class DataWarehouseManager:
    def __init__(self, server, database, username, password):
        """
        Initializes the DataWarehouseManager with database connection details.

        :param server: The server address of the SQL Server database.
        :param database: The name of the database to connect to.
        :param username: The username for database authentication.
        :param password: The password for database authentication.
        """

        self.connection = None
        self.server = server
        self.database = database
        self.username = username
        self.password = password

        log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'dwh_structure_tables_star_schema.log'))
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file_path),
                logging.StreamHandler()
            ]
        )

    def connect(self):
        try:
            self.connection = pyodbc.connect(
                f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                f'SERVER={self.server};'
                f'DATABASE={self.database};'
                f'UID={self.username};'
                f'PWD={self.password};'
            )
            logging.info("Connection to the data warehouse established successfully.")
        except Exception as e:
            logging.error(f"An error occurred while connecting to the data warehouse: {e}")

    def close_connection(self):
        """
        Establishes a connection to the SQL Server database.

        This method attempts to connect to the database using the provided credentials.
        If the connection is successful, it logs the success message. Otherwise,
        it logs the error.
        """
        if self.connection:
            self.connection.close()
            logging.info("Connection to the data warehouse closed.")

    def check_table_exists(self, dim_name):
        """
        Checks if a specified table exists in the database.

        This method queries the INFORMATION_SCHEMA.TABLES to find out if
        the specified table exists in the current database context.

        :param table_name: The name of the table to check.
        :return: True if the table exists, False otherwise.
        """
        try:
            query = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?"
            params = (dim_name,)
            result = self.execute_query(query, params)
            return result is not None and len(result) > 0
        except Exception as e:
            logging.error(f"Error checking existence of table {dim_name}: {e}")
            return False

    def execute_query(self, query, params=None):
        """
        Executes a given SQL query on the connected data warehouse.

        Before execution, it checks if the connection to the data warehouse is established.
        If the query is a 'CREATE' statement, it checks if the table already exists and
        skips creation if so. For 'SELECT' queries, it fetches and returns the results.
        For other types of queries, it executes the query and commits the changes.

        :param query: The SQL query to be executed.
        :param params: Optional parameters for the query (default is None).
        :return: The result of 'SELECT' queries, None for others.
        """
        if not self.connection:
            logging.warning("Connection not established. Please connect to the data warehouse first.")
            return

        if query.strip().lower().startswith('create'):
            dim_name = query.split()[2]
            if self.check_table_exists(dim_name):
                logging.info(f"Table {dim_name} already exists. Skipping creation.")
                return

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params) if params else cursor.execute(query)

            if query.strip().lower().startswith('select'):
                rows = cursor.fetchall()
                cursor.close()
                return rows
            else:
                self.connection.commit()
                cursor.close()
                logging.info("Query executed successfully.")

        except Exception as e:
            logging.error(f"An error occurred while executing the query: {e}")
        
    def prepare_dimension_table_sql(self, table_name, fields, primary_key):
        """
        Generate a SQL CREATE TABLE statement for a dimension table.

        :param table_name: Name of the dimension table.
        :param fields: Dictionary of fields and their types.
        :param primary_key: The field to be used as the primary key.
        :return: A SQL CREATE TABLE statement as a string.
        """
        fields_sql = [f"{field} {data_type}" for field, data_type in fields.items()]
        fields_sql.append(f"PRIMARY KEY ({primary_key})")
        fields_str = ",\n    ".join(fields_sql)
        dim_table = f"CREATE TABLE dim_{table_name.lower()} (\n    {fields_str}\n)"
        return dim_table
    
    def prepare_fact_table_sql(self, table_name, fields, primary_key, reference_tables):
        """
        Generate a SQL CREATE TABLE statement for a fact table.

        :param table_name: Name of the fact table.
        :param fields: Dictionary of fields and their types.
        :param primary_key: The field to be used as the primary key.
        :param reference_tables: List of dictionaries for foreign key references.
        :return: A SQL CREATE TABLE statement as a string.
        """
        fields_sql = [f"{field} {data_type}" for field, data_type in fields.items()]
        fields_sql.append(f"PRIMARY KEY ({primary_key})")

        for dim_table in reference_tables:
            fields_sql.append(f"FOREIGN KEY ({dim_table['fk']}) REFERENCES dim_{dim_table['table']}({dim_table['pk']})")
        
        fields_str = ",\n    ".join(fields_sql)
        dim_table = f"CREATE TABLE fact_{table_name.lower()} (\n    {fields_str}\n)"
        return dim_table

if __name__ == "__main__":
    load_dotenv('../../.env')
    server = os.getenv('DB_HOST')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')

    db_manager = DataWarehouseManager(server, database, username, password)
    db_manager.connect()

    dim_queries_ddl = {
        'part_information': {
            'fields': {
                'id': 'INT',
                'defaultPrice': 'FLOAT',
                'timeToProduce': 'FLOAT',
            },
            'id': 'id'
        },
        'material': {
            'fields': {
                'id': 'INT',
                'name': 'VARCHAR(255)',
            },
            'id': 'id'
        },        
        'material_prices': {
            'fields': {
                'id': 'INT',
                'price': 'VARCHAR(255)',
                'date': 'DATE',
            },
            'id': 'id'
        },
        'sales': {
            'fields': {
                'contractNumber': 'INT',
                'clientName': 'VARCHAR(255)',
                'date': 'DATE',
                'cash': 'FLOAT'
            },
            'id': 'contractNumber'
        },
        'machine': {
            'fields': {
                'machineId': 'INT',
            },
            'id': 'machineId'
        },            
    }

    fact_query_ddl = {
        'fields': {
            'id': 'INT',
            'machineId': 'INT',
            'timeOfProduction': 'DATE',
            'isDamaged': 'BIT',
            'partId': 'INT',
            'contractId': 'INT',
            'materialId': 'INT',
            'materialPriceId': 'INT',
        },
        'ref': [
            {
                'table': 'part_information',
                'pk': 'id',
                'fk': 'partId'
            },
            {
                'table': 'material',
                'pk': 'id',
                'fk': 'materialId'
            }, 
            {
                'table': 'material_prices',
                'pk': 'id',
                'fk': 'materialPriceId'
            },
            {
                'table': 'sales',
                'pk': 'contractNumber',
                'fk': 'contractId'
            },
            {
                'table': 'machine',
                'pk': 'machineId',
                'fk': 'machineId'
            },
        ],
        'id': 'id',
    }

    for dim_table, dim_fields in dim_queries_ddl.items():
        dim_query = db_manager.prepare_dimension_table_sql(dim_table, dim_fields['fields'], dim_fields['id'])
        db_manager.execute_query(dim_query)

    fact_query = db_manager.prepare_fact_table_sql(table_name='supply_chain',
                                                   fields=fact_query_ddl['fields'],
                                                   primary_key=fact_query_ddl['id'],
                                                   reference_tables=fact_query_ddl['ref']
                                                   )
    db_manager.execute_query(fact_query)
    db_manager.close_connection()
