import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import logging
from kafka_topic_messages_utils import append_kafka_message_to_tuples, filter_kafka_message_fields_to_push, reduce_list_records_structure, get_max_id_incremented

log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'logs', 'kafka_process_data_schema_topics_messages.log'))
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler()
            ]
    )

def process_supply_chain_topic_messages(dwh_manager, message):
    """
    Processes messages from the supply chain topic, updating the data warehouse.

    This function handles the processing of messages received from the supply chain topic.
    It performs a series of queries and inserts into the data warehouse.  
    """
    try:
        part_id_rows = """SELECT partId FROM [DWH_PRODUCTION_PROD].[dbo].[fact_sales] WHERE clientId = ?"""
        
        remaining_fields_rows = """
                            SELECT 
                                machineId, 
                                partId,
                                (SELECT MAX(unitId) FROM [DWH_PRODUCTION_PROD].[dbo].fact_supply_chain) + 
                                ROW_NUMBER() OVER (ORDER BY machineId, partId) AS unitId,
                                materialId, 
                                materialPriceId, 
                                partDefaultPrice, 
                                materialPrice
                            FROM 
                                [DWH_PRODUCTION_PROD].[dbo].fact_supply_chain
                            WHERE 
                                fact_supply_chain.partId = ?
                """
        
        logging.info('Starting to ingest Kafka part information messages in the dedicated DWH table.')

        table_name = "dim_unit"
        fields = ["id", "timeOfProduction"]
        max_unit_id_incremented = get_max_id_incremented(dwh_manager, 'id', table_name)
        records = [
            (max_unit_id_incremented[0][0], message['timeOfProduction']),
        ]

        dwh_manager.generate_and_execute_massive_insert(table_name, fields, records)

        part_id_fetched = dwh_manager.execute_query(part_id_rows, (message['order'],))
        if part_id_fetched:
            message['isDamaged'] = message['var5']

            supply_chain_result = [dwh_manager.execute_query(remaining_fields_rows, (part_id[0],)) for part_id in part_id_fetched]
            message_to_append = filter_kafka_message_fields_to_push(message, ['var5'])
            records = append_kafka_message_to_tuples(message_to_append, supply_chain_result)

            table_name = 'fact_supply_chain'
            fields = ['machineId', 'partId', 'unitId', 'materialId', 'materialPriceId', 'partDefaultPrice', 'materialPrice', 'isDamaged']
            records = reduce_list_records_structure(records)

            dwh_manager.generate_and_execute_massive_insert(table_name, fields, records)
        else:
            logging.error(f"No additional data fetched for the supply chain message. Check if {message['order']} is a referenced order number.")
        
    except Exception as e:
        logging.error(f'An unexpected error occurred while processing the message from the supply_chain topic: {e}')

def process_part_topic_messages(dwh_manager, message):
    """
    Processes messages from the part topic, inserting relevant data into the data warehouse.
    This function is responsible for handling the processing of messages received from the part topic.
    """
    try:
        logging.info('Starting to ingest Kafka supply chain messages in the dedicated DWH table.')

        table_name = "dim_part_information"
        fields = ["id", "timeToProduce"]
        max_part_id_incremented = get_max_id_incremented(dwh_manager, 'id', table_name)
        records = [
            (max_part_id_incremented[0][0], float(message['timeToProduce'])),
        ]

        return dwh_manager.generate_and_execute_massive_insert(table_name, fields, records)
    except Exception as e:
        logging.error(f'An unexpected error occurred while processing the message from the part_information topic: {e}')
    
def execute_ruling_topic_processor(dwh_manager, topic_name, message):
    """
    Executes the appropriate processor function based on the Kafka topic name.

    This function routes Kafka messages to their respective processing functions
    based on the topic name. It is a centralized handler for different topics,
    making it easier to manage the processing logic for each Kafka message type.
    """
    topic_processors = {
        'part_information': process_part_topic_messages, 
        'supply_chain': process_supply_chain_topic_messages
    }

    processor = topic_processors.get(topic_name)
    if processor:
        return processor(dwh_manager, message)
    else:
        logging.error(f"{topic_name} isn't recognized. Cannot process messages from an unreferenced topic.")