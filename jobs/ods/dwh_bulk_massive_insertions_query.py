from ods_structure_tables_star_schema import DataWarehouseManager
from dotenv import load_dotenv
import os
import logging

log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'dwh_bulk_insert.log'))
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
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

db_manager = DataWarehouseManager(server, database, username, password)
db_manager.connect()

table_name = "dim_machine"
column_names = ["machineId"]
machine_records = (1,)


massive_insert_query = db_manager.generate_massive_insert_query(table_name, column_names, machine_records)
db_manager.execute_query(massive_insert_query, params=machine_records)
db_manager.close_connection()

