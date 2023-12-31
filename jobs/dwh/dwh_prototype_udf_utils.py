import datetime
import ast

def parse_date(date_str):
    """
    A UDF (User Defined Function) that parses a date string in the format 'MM-DD-YYYY' into a datetime.date object.
    It returns `None` if the parsing fails due to a format mismatch or any other ValueError.
    """
    try:
        return datetime.datetime.strptime(date_str, '%m-%d-%Y').date()
    except ValueError as ve:
        return None

def string_to_int_list(string_list):
    """
    A UDF (User Defined Function) to convert a string representation of a list into an actual list of integers. 
    This function is particularly useful when dealing with data where lists are inconsistently represented as strings, 
    such as "['1', '2', '3']". It safely evaluates the string to a Python list using ast.literal_eval, returning an 
    empty list in case of any ValueError.
    """
    try:
        return ast.literal_eval(string_list)
    except ValueError:
        return []

def convert_timestamp_to_date(timestamp):
    """
    A UDF (User Defined Function) for converting a timestamp in milliseconds 
    to a datetime object. This function takes an integer timestamp 
    (representing the number of milliseconds since the Unix epoch, 
    January 1, 1970) and converts it into a human-readable datetime format.
    """
    return datetime.datetime.fromtimestamp(timestamp / 1000.0)

