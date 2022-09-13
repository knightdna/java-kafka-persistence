package co.novandi.diaz.utils;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import static co.novandi.diaz.config.KafkaConfig.KAFKA_BOOTSTRAP_SERVERS;
import static co.novandi.diaz.config.KafkaConfig.KAFKA_HOST;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

@UtilityClass
@Slf4j
public class KafkaUtils {

    private final String STRING_SERIALIZER_CLASS = StringSerializer.class.getName();

    public KafkaProducer<String, String> createProducer() {
        return new KafkaProducer<>(defaultProducerConfig());
    }

    public Properties defaultProducerConfig() {
        String hostname = KAFKA_HOST;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.warn("Unable to get hostname value. Reason: {}", e.getMessage());
        }

        Properties producerConfig = new Properties();
        producerConfig.put(CLIENT_ID_CONFIG, hostname);
        producerConfig.put(BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        producerConfig.put(ACKS_CONFIG, "all");

        producerConfig.put(KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER_CLASS);
        producerConfig.put(VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER_CLASS);

        return producerConfig;
    }

}
