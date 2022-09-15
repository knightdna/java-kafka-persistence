package co.novandi.diaz.client;

import co.novandi.diaz.utils.KafkaDbUtils;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Future;

import static co.novandi.diaz.config.KafkaConfig.KAFKA_TOPIC_NAME;

@RequiredArgsConstructor
@Slf4j
public class KafkaClient {

    @NonNull
    private KafkaProducer<String, String> producer;

    public void createTopic() {
        @Cleanup Admin admin = Admin.create(KafkaDbUtils.defaultAdminConfig());

        short partitions = 1;
        short replicationFactor = 1;
        NewTopic newTopic = new NewTopic(KAFKA_TOPIC_NAME, partitions, replicationFactor);

        admin.createTopics(Collections.singleton(newTopic))
                .values()
                .get(KAFKA_TOPIC_NAME)
                .whenComplete((result, createTopicsError) ->
                        Optional.ofNullable(createTopicsError).ifPresentOrElse(
                                error -> log.error("Unable to create topic {}. Reason: {}", KAFKA_TOPIC_NAME, error.getMessage()),
                                () -> log.info("Topic {} has been created successfully", KAFKA_TOPIC_NAME)));
    }

    public Future<RecordMetadata> publish(String topic, String key, String value) {
        final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        return producer.send(record);
    }

}
