def append_kafka_message_to_tuples(message, tuple_lists):
    """
    Parses a Kafka message and appends its data to each tuple in a list of lists of tuples.
    """
    message_values = tuple(message.values())
    return [[tuple(tup) + message_values for tup in lst] for lst in tuple_lists]

def filter_kafka_message_fields_to_push(message, fields_to_push):
    """
    Reconstructs a dictionary including only the selected keys.
    """
    return {
            k: message[k] 
            for k in fields_to_push if k in message
        }

def reduce_list_records_structure(records):
    """
    Flattens a nested list of records into a single list of tuples.
    """
    return [tuple for set_records in records for tuple in set_records]

def get_max_id_incremented(ods_manager, id, table_name):
    return ods_manager.execute_query(f"""SELECT MAX({id}) + 1 FROM [ODS_PRODUCTION].[dbo].[{table_name}]""")
