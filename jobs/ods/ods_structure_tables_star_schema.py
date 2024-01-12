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

        log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'ods_structure_tables_star_schema.log'))
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
            logging.info("Connection to the ODS (Operational Data Store) established successfully.")
        except Exception as e:
            logging.error(f"An error occurred while connecting to the ODS (Operational Data Store): {e}")

    def close_connection(self):
        """
        Establishes a connection to the SQL Server database.

        This method attempts to connect to the database using the provided credentials.
        If the connection is successful, it logs the success message. Otherwise,
        it logs the error.
        """
        if self.connection:
            self.connection.close()
            logging.info("Connection to the ODS (Operational Data Store) closed.")

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
        Executes a given SQL query on the connected ODS (Operational Data Store).

        Before execution, it checks if the connection to the ODS (Operational Data Store) is established.
        If the query is a 'CREATE' statement, it checks if the table already exists and
        skips creation if so. For 'SELECT' queries, it fetches and returns the results.
        For other types of queries, it executes the query and commits the changes.

        :param query: The SQL query to be executed.
        :param params: Optional parameters for the query (default is None).
        :return: The result of 'SELECT' queries, None for others.
        """
        if not self.connection:
            logging.warning("Connection not established. Please connect to the ODS (Operational Data Store) first.")
            return

        if query.strip().lower().startswith('create'):
            dim_name = query.split()[2]
            if self.check_table_exists(dim_name):
                logging.info(f"Table {dim_name} already exists. Skipping creation.")
                return
            else:
                logging.info(f"Table {dim_name} created successfully.")   

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
        if primary_key and len(primary_key) > 0:
            fields_sql.append(f"PRIMARY KEY ({primary_key})")
        fields_str = ",\n    ".join(fields_sql)
        dim_table = f"CREATE TABLE dim_{table_name.lower()} (\n    {fields_str}\n)"
        return dim_table
    
    def prepare_fact_table_sql(self, table_name, fields, cluster):
        """
        Generate a SQL CREATE TABLE statement for a fact table.

        :param table_name: Name of the fact table.
        :param fields: Dictionary of fields and their types.
        :param cluster: Dictionary with 'pk' as a list of fields for the clustered primary key 
                        and 'constraint' as the name of the constraint.
        :return: A SQL CREATE TABLE statement as a string.
        """
        fields_sql = [f"{field} {data_type}" for field, data_type in fields.items()]
        if cluster and len(cluster) > 0:
            pk_fields = ", ".join([f"{field} ASC" for field in cluster['pk']])
            fields_sql.append(f"CONSTRAINT [{cluster['constraint']}] PRIMARY KEY CLUSTERED ({pk_fields})")
            
        fields_str = ",\n    ".join(fields_sql)
        create_table_sql = f"CREATE TABLE fact_{table_name.lower()} (\n    {fields_str}\n)"
        return create_table_sql

    def generate_and_execute_massive_insert(self, table_name, column_names, records):
        """
        Generates and executes a SQL query for a massive bulk insertion using executemany.

        This function creates an INSERT INTO SQL statement and uses executemany to 
        insert multiple records into the specified table in a single operation. 
        This is more efficient for large volumes of data compared to executing 
        individual INSERT statements for each record.

        :param table_name: Name of the table to insert into.
        :param column_names: List of column names in the order corresponding to the records.
        :param records: List of tuples, each tuple representing a record to be inserted.
        """
        try:
            placeholders = ', '.join('?' * len(column_names))
            insert_query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
            cursor = self.connection.cursor()
            cursor.executemany(insert_query, records)
            self.connection.commit()
            logging.info(f"Successfully executed massive insert for table {table_name} with {len(records)} records.")
        except Exception as e:
            logging.error(f"Error executing massive insert query: {str(e)}")
            self.connection.rollback()
        finally:
            cursor.close()

if __name__ == "__main__":
    load_dotenv('../../.env')
    server = os.getenv('DB_HOST')
    database = os.getenv('DB_NAME')
    username = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')

    db_manager = DataWarehouseManager(server, database, username, password)
    db_manager.connect()

    from ods_define_star_schemas_dictionaries import dim_queries_ddl, fact_queries_ddl
    
    for dim_table, dim_fields in dim_queries_ddl.items():
        dim_query = db_manager.prepare_dimension_table_sql(dim_table, dim_fields['fields'], dim_fields['id'])
        db_manager.execute_query(dim_query)
    
    for fact_table, fact_fields in fact_queries_ddl.items():
        fact_query = db_manager.prepare_fact_table_sql(fact_table, fact_fields['fields'], fact_fields['cluster'])
        db_manager.execute_query(fact_query)
        
    db_manager.close_connection()
else:
        from .ods_define_star_schemas_dictionaries import dim_queries_ddl, fact_queries_ddl

