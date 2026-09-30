package com.example.payment;

import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.net.URI;

@Component
@EnableScheduling
public class PaymentConsumer {

    private final SqsClient sqs = createSqsClient();
    private final ExecutorService executor = Executors.newFixedThreadPool(4);

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
                .queueName("payment-queue").build()).queueUrl();

        ReceiveMessageResponse response = sqs.receiveMessage(
                ReceiveMessageRequest.builder()
                        .queueUrl(url)
                        .maxNumberOfMessages(1)
                        .waitTimeSeconds(2)
                        .visibilityTimeout(5)
                        .build());

        for (Message message : response.messages()) {
            executor.submit(() -> processMessage(url, message));
        }
    }

    private void processMessage(String url, Message message) {
        try {
            System.out.println(
                    "RECEIVED messageId=" + message.messageId()
            );

            System.out.println(
                    "[payment-queue] processing: " + message.body()
            );

            Thread.sleep(15000);

            System.out.println(
                    "DELETING messageId=" + message.messageId()
            );

            sqs.deleteMessage(
                    DeleteMessageRequest.builder()
                            .queueUrl(url)
                            .receiptHandle(message.receiptHandle())
                            .build()
            );

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Processing interrupted: " + e.getMessage());

        } catch (Exception ex) {
            System.err.println(
                    "Processing failed; message will be retried: "
                            + ex.getMessage()
            );
        }
    }
}
