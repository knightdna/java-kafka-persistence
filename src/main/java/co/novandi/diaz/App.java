package co.novandi.diaz;

import co.novandi.diaz.client.KafkaClient;
import co.novandi.diaz.client.KafkaDbClient;
import co.novandi.diaz.utils.KafkaDbUtils;
import co.novandi.diaz.utils.KafkaUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static co.novandi.diaz.config.KafkaConfig.KAFKA_TOPIC_NAME;

@Slf4j
public class App {

    private static final String AUTHOR_NAME = "anonymous";

    private final KafkaClient kafkaClient;
    private final KafkaDbClient kafkaDbClient;

    private App() {
        kafkaClient = new KafkaClient(KafkaUtils.createProducer());
        kafkaDbClient = new KafkaDbClient(KafkaDbUtils.createClient());
    }

    public void createTopic() {
        kafkaClient.createTopic();
    }

    public void createStream() {
        var createEventStreamResult = kafkaDbClient.createEventStream();
        createEventStreamResult.whenComplete((result, createStreamError) ->
                Optional.ofNullable(result).ifPresentOrElse(
                        createStreamResult -> log.info("Event stream has been successfully created. Result: {}", createStreamResult),
                        () -> log.error("Unable to create event stream. Reason: {}", createStreamError.getMessage())));
        createEventStreamResult.join();
    }

    public void publishMessage() {
        try {
            var key = "1234567";
            var value = "{" +
                    "\"id\":\"" + key + "\"," +
                    "\"author\":\"" + AUTHOR_NAME + "\"" +
                    "}";
            var recordMetadata = kafkaClient.publish(KAFKA_TOPIC_NAME, key, value)
                    .get(10, TimeUnit.SECONDS);
            log.info("Record: {}", recordMetadata);
        } catch (Exception e) {
            log.error("Unable to publish a message. Reason: {}", e.getMessage());
        }
    }

    public void findMessages() {
        var selectQueryResult = kafkaDbClient.findByAuthor(AUTHOR_NAME);
        selectQueryResult.whenComplete((result, selectQueryError) ->
                Optional.ofNullable(result).ifPresentOrElse(
                        queryResult -> {
                            log.info("Executing SELECT query");
                            queryResult.forEach(row -> log.info("Row: {}", row.values()));
                        },
                        () -> log.error("Unable to execute SELECT query. Reason: {}", selectQueryError.getMessage())));
        selectQueryResult.join();
    }

    public static void main(String[] args) {
        App app = new App();
        app.createTopic();
        app.createStream();
        app.publishMessage();
        app.findMessages();
    }

}
