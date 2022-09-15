package co.novandi.diaz.utils;

import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import lombok.experimental.UtilityClass;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static co.novandi.diaz.config.KafkaConfig.*;
import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

@UtilityClass
public class KafkaDbUtils {

    public Client createClient() {
        ClientOptions options = ClientOptions.create()
                .setHost(KSQLDB_SERVER_HOST)
                .setPort(KSQLDB_SERVER_PORT);

        return Client.create(options);
    }

    public Properties defaultAdminConfig() {
        Properties adminConfig = new Properties();
        adminConfig.put(BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        return adminConfig;
    }

    public Map<String, Object> defaultExecutionOptions() {
        return Collections.singletonMap("auto.offset.reset", "earliest");
    }

}
