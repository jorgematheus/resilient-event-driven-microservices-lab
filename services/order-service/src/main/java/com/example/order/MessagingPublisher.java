package com.example.order;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;
import software.amazon.awssdk.services.sns.model.PublishRequest;

import java.net.URI;
import java.util.Properties;

@Component
public class MessagingPublisher {

    private static final String SNS_TOPIC_ARN =
            "arn:aws:sns:us-east-1:000000000000:order-events";

    private final ObjectMapper mapper = new ObjectMapper();
    private final SnsClient sns = createSnsClient();

    private final KafkaProducer<String, String> kafka;

    private static SnsClient createSnsClient() {
        String endpoint = System.getenv().getOrDefault("AWS_ENDPOINT", "http://localhost:4566");
        SnsClientBuilder builder = SnsClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(software.amazon.awssdk.regions.Region.US_EAST_1);

        if (isLocalEndpoint(endpoint)) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                            System.getenv().getOrDefault("AWS_ACCESS_KEY_ID", "test"),
                            System.getenv().getOrDefault("AWS_SECRET_ACCESS_KEY", "test"))));
        }

        return builder.build();
    }

    private static boolean isLocalEndpoint(String endpoint) {
        String host = URI.create(endpoint).getHost();
        return "localhost".equalsIgnoreCase(host)
                || "127.0.0.1".equals(host)
                || "::1".equals(host)
                || "localstack".equalsIgnoreCase(host);
    }

    public MessagingPublisher() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        this.kafka = new KafkaProducer<>(props);
    }

    public void publish(OrderController.OrderCreated event) {
        try {
            String json = mapper.writeValueAsString(event);

            sns.publish(PublishRequest.builder()
                    .topicArn(SNS_TOPIC_ARN)
                    .message(json)
                    .build());

            kafka.send(new ProducerRecord<>("order-events", event.orderId(), json));
        } catch (Exception e) {
            throw new IllegalStateException("Unable to publish OrderCreated", e);
        }
    }
}
