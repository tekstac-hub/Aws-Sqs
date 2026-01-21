package producer;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;

public class SqsProducer {

    public static void main(String[] args) throws Exception {

        // Load properties from sqs.properties
        Properties props = new Properties();
        try (InputStream input = SqsProducer.class.getClassLoader()
                .getResourceAsStream("sqs.properties")) {

            if (input == null) {
                throw new RuntimeException("sqs.properties not found");
            }
            props.load(input);
        }

        String region = props.getProperty("aws.region");
        String queueUrl = props.getProperty("aws.sqs.queueUrl");

        // Create SQS client using region from properties
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.of(region))
                .build();

        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter messages to send to SQS. Type 'exit' to quit.");

        // Loop to read messages from user
        while (true) {
            System.out.print("Message: ");
            String message = scanner.nextLine().trim();

            if ("exit".equalsIgnoreCase(message)) {
                System.out.println("Exiting producer...");
                break;
            }

            if (!message.isEmpty()) {
                SendMessageRequest request = SendMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .messageBody(message)
                        .build();

                sqsClient.sendMessage(request);
                System.out.println("Message sent: " + message);
            }
        }

        // Cleanup
        scanner.close();
        sqsClient.close();
    }
}
