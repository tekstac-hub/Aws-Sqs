package consumer;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;

public class SqsConsumer {

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        try (InputStream input = SqsConsumer.class.getClassLoader()
                .getResourceAsStream("sqs.properties")) {

            if (input == null) {
                throw new RuntimeException("sqs.properties not found");
            }
            props.load(input);
        }

        String region = props.getProperty("aws.region");
        String queueUrl = props.getProperty("aws.sqs.queueUrl");

        SqsClient sqsClient = SqsClient.builder()
                .region(Region.of(region))
                .build();

        System.out.println("Waiting for messages...");

        while (true) { // keep polling
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10) // max allowed by SQS
                    .waitTimeSeconds(10) // long polling
                    .build();

            List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();

            if (messages.isEmpty()) {
                System.out.println("No more messages in the queue. Exiting...");
                break; // stop if queue is empty; remove this for infinite polling
            }

            for (Message message : messages) {
                System.out.println("Received message: " + message.body());

                // Delete message after processing
                sqsClient.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build());
            }
        }

        sqsClient.close();
    }
}
