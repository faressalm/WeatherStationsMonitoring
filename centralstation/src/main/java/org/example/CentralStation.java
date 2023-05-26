package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;


public class CentralStation {
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
        while (true) {
            ConsumerRecords<String, String> messages = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : messages) {
                // Convert the JSON string to an object
                ObjectMapper objectMapper = new ObjectMapper();
                SensorData sensorData = null;
                try {
                    sensorData = objectMapper.readValue(record.value(), SensorData.class);
                    System.out.println("Received: " + sensorData);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
