package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.util.Properties;

public class RainDetector {
    public static void main(String[] args) {
        final String INPUT_TOPIC = "sensor-data-topic";
        final String OUTPUT_TOPIC = "raining-topic";

        System.out.println("Starting Rain Detector...");

        // Set up Kafka Streams configuration
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "rain-detector-app");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);

        // Process the message stream
        inputStream.filter((key, value) -> {
            ObjectMapper objectMapper = new ObjectMapper();
            SensorData sensorData = null;
            try {
                sensorData = objectMapper.readValue(value, SensorData.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            // Check if humidity is higher than 70%
            return sensorData.getWeather().getHumidity() > 70;
        }).mapValues(value -> {
            // Generate the special message
            ObjectMapper objectMapper = new ObjectMapper();
            SensorData sensorData = null;
            try {
                sensorData = objectMapper.readValue(value, SensorData.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return "Humidity is too high! for station "+ sensorData.getStationId();
        }).to(OUTPUT_TOPIC); // Publish the special message to the output topic

        // Build the topology and start Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();
    }
}
