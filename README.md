# Weather Stations Monitoring Project

This project aims to monitor weather stations and analyze their data. The project includes several components and technologies, including Kafka, Kafka Streams, Bitcask Riak, Parquet, Elasticsearch, Kibana, and Kubernetes.

## Contributors

The project was developed by:

- Fares Waheed Abd-El Hakim
- Ali Ahmed Ibrahim Fahmy
- Ahmed Mohamed Abd-El Monem
- Basel Ayman Mohamed

## Weather Station Mock

The Weather Station Mock is a Java program that generates simulated weather sensor data and sends it to a Kafka topic. The data is sent as JSON strings, and the code uses the Jackson library to convert the `SensorData` object to a JSON string. The code is designed to run indefinitely and sends data at one message per second.

## Weather Station Connection to Kafka

The Weather Station Connection to Kafka is a Java program that connects the Weather Station Mock to Kafka. The code uses the Kafka Producer API to send data to `sensor-data-topic`.

## Raining Triggers in Kafka Processor

The Raining Triggers in Kafka Processor is a Kafka Streams application written in Java. The RainDetector application reads data from an input topic "`sensor-data-topic`" and filters the data to include only data where the humidity level is greater than 70%. The filtered data is mapped to a new message with a special message indicating that **humidity is too high for the station that sent the data.** The new message is then published to the output topic "`raining-topic`".

## Central Station

The Central Station is a Java program that consumes messages from Kafka and stores updated views of weather statuses in BitCask Riak and archives the weather statuses in Parquet files. The code enters an infinite loop with while (true) to continuously consume messages from Kafka. In each iteration of the loop, we use the poll() method to retrieve ConsumerRecords from Kafka. For each ConsumerRecord in the messages collection, we store updated views of weather statuses in BitCask Riak and archive the weather statuses in Parquet files.

### Bitcask Riak

Bitcask Riak is a key-value store that stores weather data. The key is a directory, and the value is a string. We created a hash map for the key directory to allow for random access of files during get operations from the key-value store. We created a replica directory to replicate that data to run the compaction over it. We spawned a new thread to do the compaction on a daily basis. We made the threshold of the active file to be 0.1 of the records/day. We created a hint file for the output of the compaction, allowing for recovering from crush/shutdown using it to retrieve the key directory easily.

### Archiving with Parquet

We are consuming messages from Kafka and writing them to a Parquet file in batches and partition these parquet files by time and station ID. We have a “recordBatch” variable, which is a HashMap that stores records grouped by a key of type Long (Station ID). Each key is associated with a list of GenericData.Record objects (All the records from this station in one batch). There's a “batchSize” variable that determines the number of records before writing to the Parquet file. We call the setAvroRecord() method to add the “sensorData” object (readValue() method of the ObjectMapper used to convert the JSON message into it) to the “recordBatch” map. If the processed count reaches the “batchSize”, write the current batch to the Parquet file using the writeRecordBatch() method and clear the “recordBatch” map.

## Historical Weather Statuses Analysis

The Historical Weather Statuses Analysis is a Python script that monitors a specified folder “`folder_path`" for new Parquet files and indexes each row in the files to an Elasticsearch instance. The Elasticsearch connection is configured by creating an `Elasticsearch` instance with the URL of the Elasticsearch instance. The folder to monitor is specified with the variable. The event handler `ParquetFileHandler` is defined, which extends `FileSystemEventHandler` and overrides the `on_closed` method. This method is called whenever a file in the monitored folder is closed. When a Parquet file is closed, the `on_closed` method checks if the file exists and is not empty. If the file is valid, it reads the file into a Pandas DataFrame. Each row in the DataFrame is indexed to the Elasticsearch instance with a unique ID created from the `station_id` and `s_no` columns.

## Deploy using Kubernetes

We deployed the components of the project using Kubernetes. We created a docker-compose.yaml file that combined all of the services images, then used `kmpose convert` to convert this docker compose to Kubernetes yaml files. We used `minikube start` to run our cluster. We used `kubectl apply` to make our deployments running on Kubernetes.

## Profile Central Station using JFR

We used Java Flight Recorder (JFR) to collect diagnostic and profile data about a running central station. First, we run (jcmd) to get the process id of the central station. Then we run the command line (jcmd <PID> JFR.start duration=60s filename=flight.jfr) to generate a flight.jfr file. We opened this file with the JDK Mission Controller (JMC) and viewed Top 10 Classes with the highest total memory from the "Memory" tab, GC pauses count and GC maximum pause duration from the "Garbage Collections" tab, and a list of I/O operations from the "I/O" tab.
