import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import logging
from kafka_topic_messages_utils import append_kafka_message_to_tuples, filter_kafka_message_fields_to_push, get_max_id_incremented, get_ods_table_id, delete_ods_table_records

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
        table_name = 'fact_supply_chain'
        if message['isDeleted']:
            logging.info(f'Attempting to delete records for table {table_name} in the dedicated ODS table.')
            query_to_delete = f"""
                                DELETE FROM [ODS_PRODUCTION].[dbo].[{table_name}] 
                                WHERE [{table_name}].[trscMachineId] = ?
                                AND [{table_name}].[trscPartId] = ?
                                AND [{table_name}].[timeOfProduction] = ?
                                """
            return ods_manager.execute_query(query_to_delete, (message['machineId'], message['partId'], message['timeOfProduction'],))

        supply_chain_query = f"""
                            SELECT DISTINCT
                                materialId,
                                materialPrice,
                                materialPriceDate,
                                partDefaultPrice 
                            FROM 
                                [ODS_PRODUCTION].[dbo].[{table_name}]
                            WHERE 
                                [{table_name}].[partId] = ?
                            AND
                                YEAR([{table_name}].[materialPriceDate]) = YEAR(?)
                            """
        material_operational_id = f"""SELECT trscMaterialId FROM [ODS_PRODUCTION].[dbo].[dim_material] WHERE materialId = ?"""

        logging.info('Starting to ingest Kafka part information messages in the dedicated ODS table.')

        materials_fetched = ods_manager.execute_query(supply_chain_query, (message['partId'], message['timeOfProduction']))
        if materials_fetched:
            tuple_materials = [tuple(material) for material in materials_fetched]
            for idx, material in enumerate(materials_fetched):
                trsc_material_id = ods_manager.execute_query(material_operational_id, (material[0],))[0][0]
                operational_material_id = trsc_material_id if trsc_material_id is not None else get_max_id_incremented(ods_manager, 'trscMaterialId', 'dim_material')[0][0] + idx
                tuple_materials[idx] += (operational_material_id,)
            
            message['machineIdODS'] = get_ods_table_id(ods_manager, 'machineId', message['machineId'], 'dim_machine')[0][0]
            message['partIdODS'] = get_ods_table_id(ods_manager, 'partId', message['partId'], 'dim_part_information')[0][0]
            
            message['timeId'] = int(message['timeOfProduction'].split('T')[0].replace('-', ''))
            message['isDamaged'] = message['var5']

            message_to_append = filter_kafka_message_fields_to_push(message, ['machineIdODS', 'partIdODS', 'materialIdODS', 'timeId', 'machineId', 'partId', 'timeOfProduction', 'isDamaged', 'lastUpdate'])
            records = append_kafka_message_to_tuples(message_to_append, tuple_materials)
            
            fields = ['materialId', 'materialPrice', 'materialPriceDate', 'partDefaultPrice', 'trscMaterialId', 'machineId', 'partId', 'timeId', 'trscMachineId', 'trscPartId', 'timeOfProduction', 'isDamaged', 'lastUpdate']

            ods_manager.generate_and_execute_massive_insert(table_name, fields, records)
        else:
            logging.error(f"No additional data fetched for the supply chain message. Check if {message['partId']} is a referenced part number.")
        
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
            logging.info(f'Attempting to delete records for table {table_name} in the dedicated ODS table.')
            return delete_ods_table_records(ods_manager, 'partId', message['id'], table_name)

        logging.info('Starting to ingest Kafka part_information messages in the dedicated ODS table.')

        fields = ["trscPartId", "partId", "timeToProduce", "lastUpdate"]
        part_id = get_ods_table_id(ods_manager, 'partId', message['id'], table_name)[0][0]
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
            logging.info(f'Attempting to delete records for table {table_name} in the dedicated ODS table.')
            return delete_ods_table_records(ods_manager, 'machineId', message['id'], table_name)
        
        logging.info('Starting to ingest Kafka machine messages in the dedicated ODS table.')

        fields = ['trscMachineId', 'machineId', 'lastUpdate']
        machine_id = get_ods_table_id(ods_manager, 'machineId', message['id'], table_name)[0][0]
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
            logging.info(f'Attempting to delete records for table {table_name} in the dedicated ODS table.')
            return delete_ods_table_records(ods_manager, 'materialId', message['id'], table_name)

        logging.info('Starting to ingest Kafka material messages in the dedicated ODS table.')

        fields = ['trscMaterialId', 'materialId', 'name', 'lastUpdate']
        material_id = get_ods_table_id(ods_manager, 'materialId', message['id'], table_name)[0][0]
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
            logging.info(f'Attempting to delete records for table {table_name} in the dedicated ODS table.')
            return delete_ods_table_records(ods_manager, 'contractId', message['id'], table_name)

        logging.info('Starting to ingest Kafka contract messages in the dedicated ODS table.')

        fields = ["trscContractId", "contractId", "clientName", "lastUpdate"]
        contract_id = get_ods_table_id(ods_manager, 'contractId', message['id'], table_name)[0][0]
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
        'contract': process_contract_topic_messages
    }

    processor = topic_processors.get(topic_name)
    if processor:
        return processor(ods_manager, message)
    else:
        logging.error(f"{topic_name} isn't recognized. Cannot process messages from an unreferenced topic.")