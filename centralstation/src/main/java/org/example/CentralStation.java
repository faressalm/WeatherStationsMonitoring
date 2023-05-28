package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;


public class CentralStation {
    private static final String OUTPUT_DIRECTORY = "/usr/app/weather_monitoring/stations_parquet_log";

    // Create Avro schema
    private static final Schema avroSchema = new Schema.Parser().parse(
            "{" +
                    "\"type\": \"record\"," +
                    "\"name\": \"WeatherStatus\"," +
                    "\"fields\": [" +
                    "{\"name\": \"station_id\", \"type\": \"long\"}," +
                    "{\"name\": \"s_no\", \"type\": \"long\"}," +
                    "{\"name\": \"battery_status\", \"type\": \"string\"}," +
                    "{\"name\": \"status_timestamp\", \"type\": \"long\"}," +
                    "{\"name\": \"weather\", \"type\": {" +
                    "\"type\": \"record\"," +
                    "\"name\": \"WeatherInfo\"," +
                    "\"fields\": [" +
                    "{\"name\": \"humidity\", \"type\": \"int\"}," +
                    "{\"name\": \"temperature\", \"type\": \"int\"}," +
                    "{\"name\": \"wind_speed\", \"type\": \"int\"}" +
                    "]" +
                    "}}" +
                    "]" +
                    "}"
    );
    public static void main(String[] args) {
        final String TOPIC_NAME = "sensor-data-topic";
        System.out.println("Starting Kafka consumer...");
        // Set up Kafka consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        Consumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        Bitcask bitcask = new Bitcask();
        // Consume messages from Kafka and write to Parquet
        Map<Long, List<GenericData.Record>> recordBatch = new HashMap<>();
        int batchSize = 100;
        int processedCount = 0;
        // invoke thread to compact on replica
        bitcask.run_compaction();
        while (true) {
            ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : messages) {
                // Convert the JSON string to an object
                ObjectMapper objectMapper = new ObjectMapper();
                SensorData sensorData = null;
                try {
                    sensorData = objectMapper.readValue(record.value(), SensorData.class);
                    // Parquet
                    setAvroRecord(sensorData, recordBatch);
                    if (++processedCount >= batchSize) {
                        // Write the batch to Parquet file
                        writeRecordBatch(recordBatch);
                        recordBatch.clear();
                        System.out.println("Processed records count: " + processedCount);
                        processedCount = 0;
                    }
                    // BitCask
                    bitcask.put(Math.toIntExact(sensorData.getStationId()), record.value());
                    System.out.println("BitCask output" + bitcask.get(1));
                } catch (Exception e) {
                    bitcask.shut_down();
                    e.printStackTrace();
                }
            }
        }

    }

    public static void setAvroRecord(SensorData sensorData, Map<Long, List<GenericData.Record>> recordBatch){
        GenericData.Record rec = new GenericData.Record(avroSchema);

        // Set field values in the record builder
        rec.put("station_id", sensorData.getStationId());
        rec.put("s_no", sensorData.getSNo());
        rec.put("battery_status", sensorData.getBatteryStatus());
        rec.put("status_timestamp", sensorData.getStatusTimestamp());

        GenericData.Record weatherInfo = new GenericData.Record(avroSchema.getField("weather").schema());
        weatherInfo.put("humidity", sensorData.getWeather().getHumidity());
        weatherInfo.put("temperature", sensorData.getWeather().getTemperature());
        weatherInfo.put("wind_speed", sensorData.getWeather().getWindSpeed());
        rec.put("weather", weatherInfo);
        recordBatch.computeIfAbsent(sensorData.getStationId(), key -> new ArrayList<>()).add(rec);

    }
    private static void writeRecordBatch(Map<Long, List<GenericData.Record>> recordBatch) {
        try {
            // Get current timestamp
            long timestamp = System.currentTimeMillis();

            // Convert timestamp to Date object
            Date date = new Date(timestamp);

            // Define the desired date and time format
            String format = "yyyy-MM-dd_HH-mm-ss";

            // Create a SimpleDateFormat object with the desired format
            SimpleDateFormat sdf = new SimpleDateFormat(format);

            // Format the date and time
            String formattedDateTime = sdf.format(date);

            for (Map.Entry<Long, List<GenericData.Record>> entry : recordBatch.entrySet()) {
                // Create Parquet writer
                ParquetWriter<GenericData.Record> writer = createParquetWriter(entry.getKey(), formattedDateTime);

                for (GenericData.Record record : entry.getValue()) {
                    writer.write(record);
                }
                writer.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write record batch to Parquet file", e);
        }
    }
    private static ParquetWriter<GenericData.Record> createParquetWriter(Long stationId, String dateTime) {
        File outputDir = new File(OUTPUT_DIRECTORY);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        outputDir = new File(OUTPUT_DIRECTORY + "/station" + stationId);
        if (!outputDir.exists()) {
            outputDir.mkdirs();
        }

        String parquetFilePath = OUTPUT_DIRECTORY + "/station" + stationId + File.separator + dateTime + ".parquet";
        ParquetWriter<GenericData.Record> writer;
        try {
            writer = AvroParquetWriter
                    .<GenericData.Record>builder(new org.apache.hadoop.fs.Path(parquetFilePath))
                    .withSchema(avroSchema)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withConf(new Configuration())
                    .build();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create Parquet writer", e);
        }

        return writer;
    }
}
