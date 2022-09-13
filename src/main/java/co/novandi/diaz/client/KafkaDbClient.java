package co.novandi.diaz.client;

import co.novandi.diaz.utils.KafkaDbUtils;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
public class KafkaDbClient {

    private static final String CREATE_EVENT_STREAM = """
            CREATE STREAM \"event_stream\" 
            (\"ID\" VARCHAR, \"AUTHOR\" VARCHAR) 
            WITH (KAFKA_TOPIC='event_topic', VALUE_FORMAT='JSON');
            """;

    private static final String FIND_EVENTS_BY_AUTHOR = "SELECT * FROM \"event_stream\" WHERE AUTHOR='%s';";

    @NonNull
    private Client dbClient;

    public CompletableFuture<ExecuteStatementResult> createEventStream() {
        return dbClient.executeStatement(CREATE_EVENT_STREAM, KafkaDbUtils.defaultExecutionOptions());
    }

    public BatchedQueryResult findByAuthor(String author) {
        return dbClient.executeQuery(String.format(FIND_EVENTS_BY_AUTHOR, author), KafkaDbUtils.defaultExecutionOptions());
    }

}
