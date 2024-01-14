import logging
import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

def upload_to_azure_blob(account_name, account_key, container_name, blob_name, file_path):
    """
    Uploads a file to an Azure Blob Storage container with logging. It will persist all the files version history of the
    supply chain and sales CSV/Excel data in a cool level storage tier in order to prioritize high availability against
    low latency access to the resources. This will ensure that none of the files are corrupted in addition of the replication
    in a LRS (Local Redundant Storage) strategy.
    """

    try:
        logging.info("Connecting to Azure Blob Storage...")
        blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net", 
            credential=account_key
        )

        logging.info(f"Creating blob client for container: {container_name}, blob: {blob_name}...")
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        timeout = 10 * 60

        logging.info(f"Uploading file '{file_path}' to Azure Blob Storage...")
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, blob_type="BlockBlob", timeout=timeout)
        
        logging.info(f"File '{file_path}' successfully uploaded as '{blob_name}' in container '{container_name}'.")
    
    except ResourceExistsError:
        logging.info(f"File '{file_path}' already exists in container '{container_name}'. Skipping upload.")

    except Exception as e:
        logging.error(f"Error occurred during blob upload: {e}")

def upload_directory(account_name, account_key, container_name, directory_path):
    for item_name in os.listdir(directory_path):
        item_path = os.path.join(directory_path, item_name)

        if os.path.isfile(item_path):
            logging.info(f"Uploading file: {item_name}...")
            upload_to_azure_blob(account_name, account_key, container_name, item_name, item_path)
        elif os.path.isdir(item_path):
            logging.info(f"Entering directory: {item_name}...")
            upload_directory(account_name, account_key, container_name, item_path)

if __name__ == "__main__":
    log_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'logs', 'store_files_into_blob_container_azure.log'))
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
                    logging.FileHandler(log_file_path),
                    logging.StreamHandler()
                ]
        )

    load_dotenv('../../../.env')
    account_name = os.getenv('AZURE_BLOB_STORAGE_ACCOUNT')
    account_key = os.getenv('AZURE_BLOB_ACCESS_KEY')
    container_name = os.getenv('AZURE_CONTAINER_NAME')
    data_directory = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'data')

    upload_directory(account_name, account_key, container_name, data_directory)
