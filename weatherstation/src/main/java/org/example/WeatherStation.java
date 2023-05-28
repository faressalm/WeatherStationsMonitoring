package org.example;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class WeatherStation {
    public static void main(String[] args) {
        String topicName = "sensor-data-topic";
        String bootstrapServers = "kafka:9092";
        ObjectMapper objectMapper = new ObjectMapper();
        Random random = new Random();
        AtomicLong messageCount = new AtomicLong(1L);
        // Read the station ID from the environment variable
        String stationIdEnv = System.getenv("STATION_ID");
        if (stationIdEnv == null || stationIdEnv.isEmpty()) {
            throw new IllegalArgumentException("STATION_ID environment variable is not set");
        }
        long stationId = Long.parseLong(stationIdEnv);

        // Kafka producer configuration
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
            executorService.scheduleAtFixedRate(() -> {
                // Create an instance of the SensorData object
                SensorData sensorData = new SensorData();
                sensorData.setStationId(stationId);
                sensorData.setSNo(messageCount.getAndIncrement());
                sensorData.setStatusTimestamp(System.currentTimeMillis());
                SensorData.Weather weather = new SensorData.Weather();
                weather.setHumidity(random.nextInt(101));
                weather.setTemperature(random.nextInt(101));
                weather.setWindSpeed(random.nextInt(101));
                sensorData.setWeather(weather);

                // Randomly change the battery_status field
                int batteryStatusChoice = random.nextInt(10);
                if (batteryStatusChoice < 3) {
                    sensorData.setBatteryStatus("low");
                } else if (batteryStatusChoice < 7) {
                    sensorData.setBatteryStatus("medium");
                } else {
                    sensorData.setBatteryStatus("high");
                }

                try {
                    // Convert the SensorData object to JSON string
                    String json = objectMapper.writeValueAsString(sensorData);

                    // Randomly drop messages on a 10% rate
                    if (random.nextDouble() < 0.1) {
                        System.out.println("Dropped: " + json);
                        return; // Skip sending the message to Kafka
                    }

                    // Create a ProducerRecord with the topic and JSON string as value
                    ProducerRecord<String, String> record = new ProducerRecord<>(topicName, json);

                    // Send the record
                    producer.send(record);
                    System.out.println("Sent: " + json);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, 0, 10, TimeUnit.MILLISECONDS);

            // Sleep indefinitely to keep the producer running
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Client Out");
    }
}
