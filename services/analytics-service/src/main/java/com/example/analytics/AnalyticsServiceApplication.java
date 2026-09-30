package com.example.analytics;

import org.apache.kafka.clients.consumer.*;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@SpringBootApplication
public class AnalyticsServiceApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsServiceApplication.class, args);
    }

    @Override
    public void run(String... args) {
        Thread.ofVirtual().start(() -> {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092"));
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "analytics-service");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(List.of("order-events"));

                while (true) {
                    for (ConsumerRecord<String, String> record :
                            consumer.poll(Duration.ofMillis(1000))) {
                        System.out.printf(
                                "[analytics] partition=%d offset=%d key=%s value=%s%n",
                                record.partition(), record.offset(),
                                record.key(), record.value());
                    }
                }
            }
        });
    }
}
