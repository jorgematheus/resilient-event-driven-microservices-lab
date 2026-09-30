package com.example.inventory;

import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.*;

import java.net.URI;

@Component
@EnableScheduling
public class InventoryConsumer {

    private final SqsClient sqs = createSqsClient();

    private static SqsClient createSqsClient() {
        String endpoint = System.getenv().getOrDefault("AWS_ENDPOINT", "http://localhost:4566");
        SqsClientBuilder builder = SqsClient.builder()
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

    @Scheduled(fixedDelay = 1000)
    public void poll() {
        String url = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                .queueName("inventory-queue").build()).queueUrl();

        ReceiveMessageResponse response = sqs.receiveMessage(
                ReceiveMessageRequest.builder()
                        .queueUrl(url)
                        .maxNumberOfMessages(10)
                        .waitTimeSeconds(10)
                        .visibilityTimeout(30)
                        .build());

        for (Message message : response.messages()) {
            try {
                System.out.println("[inventory-queue] processing: " + message.body());

                // TODO: replace with real business processing.
                // For the resilience experiment, throw an exception here
                // and observe SQS redelivery / DLQ behavior.

                sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(url)
                        .receiptHandle(message.receiptHandle())
                        .build());
            } catch (Exception ex) {
                System.err.println("Processing failed; message will be retried: "
                        + ex.getMessage());
            }
        }
    }
}
