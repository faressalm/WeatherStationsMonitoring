import os
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from elasticsearch import Elasticsearch
import pandas as pd

# Configure Elasticsearch connection
es = Elasticsearch(['http://localhost:9200'])
index_name = 'weather_statuses_index'  # Specify the Elasticsearch index name

# Define the folder path to monitor
folder_path = '/app'


class ParquetFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            # Traverse through subdirectories
            for root, dirs, files in os.walk(event.src_path):
                for file in files:
                    file_path = os.path.join(root, file)
                    if file_path.lower().endswith('.parquet'):
                        print('New Parquet file detected: %s' % file_path)
                        # Read the Parquet file into a Pandas DataFrame
                        df = pd.read_parquet(file_path)

                        # Index each row in the DataFrame to Elasticsearch
                        for _, row in df.iterrows():
                            doc = row.to_dict()
                            es.index(index=index_name, document=doc)
                        return


# Create the event handler and observer
event_handler = ParquetFileHandler()
observer = Observer()

# Schedule the observer to watch the folder path
observer.schedule(event_handler, path=folder_path, recursive=True)

# Print a message indicating that the program is ready to listen
print("Program is ready to listen for updates in the folder:", folder_path)

# Start the observer in a separate thread
observer.start()

try:
    # Keep the main thread alive
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    # Stop the observer when interrupted
    observer.stop()

# Wait for the observer's thread to finish
observer.join()
