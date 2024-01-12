import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import logging
from kafka_topic_messages_utils import append_kafka_message_to_tuples, filter_kafka_message_fields_to_push, reduce_list_records_structure, get_max_id_incremented, get_dim_ods_table_id, delete_dim_ods_table_records

log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'kafka_process_data_schema_topics_messages.log'))
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler()
            ]
    )

def process_supply_chain_topic_messages(ods_manager, message):
    """
    Processes messages from the supply chain topic, updating the ODS (Operational Data Store).

    This function handles the processing of messages received from the supply chain topic.
    It performs a series of queries and inserts into the ODS (Operational Data Store).  
    """
    try:
        part_id_rows = """SELECT partId FROM [ODS_PRODUCTION].[dbo].[fact_sales] WHERE trscContractId = ?"""
        
        remaining_fields_rows = """
                            SELECT 
                                machineId, 
                                partId,
                                materialId,
                                timeId,
                                materialPriceDate,
                                partDefaultPrice, 
                                materialPrice
                            FROM 
                                [ODS_PRODUCTION].[dbo].fact_supply_chain
                            WHERE 
                                fact_supply_chain.partId = ?
                            AND
                                YEAR(fact_supply_chain.materialPriceDate) = YEAR(?)
                """
        
        logging.info('Starting to ingest Kafka part information messages in the dedicated ODS table.')

        part_id_fetched = ods_manager.execute_query(part_id_rows, (message['order'], message['timeOfProduction']))
        if part_id_fetched:
            message['isDamaged'] = message['var5']

            supply_chain_result = [ods_manager.execute_query(remaining_fields_rows, (part_id[0],)) for part_id in part_id_fetched]
            message_to_append = filter_kafka_message_fields_to_push(message, ['var5'])
            records = append_kafka_message_to_tuples(message_to_append, supply_chain_result)

            table_name = 'fact_supply_chain'
            fields = ['machineId', 'partId', 'materialId', 'timeId', 'materialPriceDate', 'partDefaultPrice', 'materialPrice', 'isDamaged']
            records = reduce_list_records_structure(records)

            ods_manager.generate_and_execute_massive_insert(table_name, fields, records)
        else:
            logging.error(f"No additional data fetched for the supply chain message. Check if {message['order']} is a referenced order number.")
        
    except Exception as e:
        logging.error(f'An unexpected error occurred while processing the message from the supply_chain topic: {e}')

def process_part_topic_messages(ods_manager, message):
    """
    Processes messages from the part topic, inserting relevant data into the ODS (Operational Data Store).
    This function is responsible for handling the processing of messages received from the part topic.
    """
    try:
        table_name = "dim_part_information"

        if message['isDeleted']:
            logging.info(f'Successfully deleted records for table {table_name} in the dedicated ODS table.')
            return delete_dim_ods_table_records(ods_manager, 'partId', message['id'], table_name)

        logging.info('Starting to ingest Kafka part_information messages in the dedicated ODS table.')

        fields = ["trscPartId", "partId", "timeToProduce", "lastUpdate"]
        part_id = get_dim_ods_table_id(ods_manager, 'partId', message['id'], table_name)[0][0]
        records = [
            (message['id'], part_id, float(message['timeToProduce']), message['lastUpdate']),
        ]

        return ods_manager.generate_and_execute_massive_insert(table_name, fields, records)
    except Exception as e:
        logging.error(f'An unexpected error occurred while processing the message from the part_information topic: {e}')

def process_machine_topic_messages(ods_manager, message):
    """
    Processes messages from the machine topic, updating the ODS (Operational Data Store).
    """
    try:
        table_name = 'dim_machine'

        if message['isDeleted']:
            logging.info(f'Successfully deleted records for table {table_name} in the dedicated ODS table.')
            return delete_dim_ods_table_records(ods_manager, 'machineId', message['id'], table_name)
        
        logging.info('Starting to ingest Kafka machine messages in the dedicated ODS table.')

        fields = ['trscMachineId', 'machineId', 'lastUpdate']
        machine_id = get_dim_ods_table_id(ods_manager, 'machineId', message['id'], table_name)[0][0]
        records = [
            (message['id'], machine_id, message['lastUpdate'],),
        ]

        return ods_manager.generate_and_execute_massive_insert(table_name, fields, records)
    except Exception as e:
        logging.error(f'An unexpected error occurred while processing the message from the machine topic: {e}')

def process_material_topic_messages(ods_manager, message):
    """
    Processes messages from the machine topic, updating the ODS (Operational Data Store).
    """
    try:
        table_name = 'dim_material'

        if message['isDeleted']:
            logging.info(f'Successfully deleted records for table {table_name} in the dedicated ODS table.')
            return delete_dim_ods_table_records(ods_manager, 'materialId', message['id'], table_name)

        logging.info('Starting to ingest Kafka material messages in the dedicated ODS table.')

        fields = ['trscMaterialId', 'materialId', 'name', 'lastUpdate']
        material_id = get_dim_ods_table_id(ods_manager, 'materialId', message['id'], table_name)[0][0]
        records = [
            (message['id'], material_id, message['name'], message['lastUpdate']),
        ]

        return ods_manager.generate_and_execute_massive_insert(table_name, fields, records)
    except Exception as e:
        logging.error(f'An unexpected error occurred while processing the message from the material topic: {e}')

def process_contract_topic_messages(ods_manager, message):
    """
    Processes messages from the contract topic, inserting relevant data into the ODS (Operational Data Store).
    This function is responsible for handling the processing of messages received from the contract topic.
    """
    try:
        table_name = "dim_contract"

        if message['isDeleted']:
            logging.info(f'Successfully deleted records for table {table_name} in the dedicated ODS table.')
            return delete_dim_ods_table_records(ods_manager, 'contractId', message['id'], table_name)

        logging.info('Starting to ingest Kafka contract messages in the dedicated ODS table.')

        fields = ["trscContractId", "contractId", "clientName", "lastUpdate"]
        contract_id = get_dim_ods_table_id(ods_manager, 'contractId', message['id'], table_name)[0][0]
        records = [
            (message['id'], contract_id, message['clientName'], message['lastUpdate']),
        ]

        return ods_manager.generate_and_execute_massive_insert(table_name, fields, records)
    except Exception as e:
        logging.error(f'An unexpected error occurred while processing the message from the contract topic: {e}')


def execute_ruling_topic_processor(ods_manager, topic_name, message):
    """
    Executes the appropriate processor function based on the Kafka topic name.

    This function routes Kafka messages to their respective processing functions
    based on the topic name. It is a centralized handler for different topics,
    making it easier to manage the processing logic for each Kafka message type.
    """
    topic_processors = {
        'part_information': process_part_topic_messages, 
        'supply_chain': process_supply_chain_topic_messages,
        'machine': process_machine_topic_messages,
        'material': process_material_topic_messages,
        'material_prices': process_supply_chain_topic_messages,
        'contract': process_contract_topic_messages
    }

    processor = topic_processors.get(topic_name)
    if processor:
        return processor(ods_manager, message)
    else:
        logging.error(f"{topic_name} isn't recognized. Cannot process messages from an unreferenced topic.")