import os
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from elasticsearch import Elasticsearch
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Elasticsearch connection
es = None
while es is None:
    try:
        es = Elasticsearch(['http://elasticsearch:9200'])
        if not es.ping():
            es = None
            raise ConnectionError("Invalid Elasticsearch instance")
    except ConnectionError as e:
        logger.warning("Failed to connect to Elasticsearch. Retrying in 5 seconds...")
        time.sleep(5)


logger.info("Connected to Elasticsearch")

index_name = 'weather_statuses_index'  # Specify the Elasticsearch index name

# Define the folder path to monitor
folder_path = '/app'


class ParquetFileHandler(FileSystemEventHandler):
    def on_closed(self, event):
        file_path = event.src_path
        if file_path.lower().endswith('.parquet'):
            logger.info('Parquet file closed: %s' % file_path)
            # Check if the Parquet file exists and is not empty
            if os.path.isfile(file_path) and os.path.getsize(file_path) > 0:
                logger.info('Processing Parquet file: %s' % file_path)
                try:
                    logger.info('Reading Parquet file: %s' % file_path)
                    # Read the Parquet file into a Pandas DataFrame
                    df = pd.read_parquet(file_path)

                    # Index each row in the DataFrame to Elasticsearch
                    for _, row in df.iterrows():
                        logger.info('Indexing row: %s' % row)
                        doc = row.to_dict()
                        station_id = doc.get('station_id')
                        s_no = doc.get('s_no')
                        document_id = f"station_{station_id}-{s_no}"
                        es.index(index=index_name, id=document_id, body=doc)
                except Exception as e:
                    logger.error("Error processing Parquet file: %s" % e)
            else:
                logger.error("Empty Parquet file. Skipping processing.")



# Create the event handler and observer
event_handler = ParquetFileHandler()
observer = Observer()

# Schedule the observer to watch the folder path
observer.schedule(event_handler, path=folder_path, recursive=True)

# Print a message indicating that the program is ready to listen
logger.info("Program is ready to listen for updates in the folder: %s", folder_path)

# Start the observer in a separate thread
observer.start()

try:
    # Keep the main thread alive
    while True:
        observer.join(timeout=0.1)
except KeyboardInterrupt:
    # Stop the observer when interrupted
    observer.stop()

# Wait for the observer's thread to finish
observer.join()
