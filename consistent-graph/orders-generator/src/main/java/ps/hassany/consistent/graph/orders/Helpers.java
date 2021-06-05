package ps.hassany.consistent.graph.orders;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.AdminClient;
import ps.hassany.consistent.graph.common.SchemaPublication;
import ps.hassany.consistent.graph.common.TopicCreation;
import ps.hassany.consistent.graph.common.TopicsCreationConfig;
import ps.hassany.consistent.graph.domain.DomainGraphRecord;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class Helpers {
  private final OrdersGeneratorAppConfig appConfig;
  private final AdminClient adminClient;
  private final CachedSchemaRegistryClient schemaRegistryClient;

  public Helpers(
      OrdersGeneratorAppConfig appConfig,
      AdminClient adminClient,
      CachedSchemaRegistryClient schemaRegistryClient) {
    this.appConfig = appConfig;
    this.adminClient = adminClient;
    this.schemaRegistryClient = schemaRegistryClient;
  }

  public void createTopics() throws ExecutionException, InterruptedException {
    List<TopicsCreationConfig> topicConfigs =
        List.of(
            new TopicsCreationConfig(
                appConfig.getOrdersTopicName(),
                appConfig.getOrdersTopicPartitions(),
                appConfig.getOrdersTopicReplicationFactor()),
            new TopicsCreationConfig(
                appConfig.getOrdersNodesTopicName(),
                appConfig.getOrdersNodesTopicPartitions(),
                appConfig.getOrdersNodesTopicReplicationFactor(),
                Optional.of(Map.of("cleanup.policy", "compact"))));
    TopicCreation topicCreation = new TopicCreation(adminClient);
    topicCreation.createTopics(topicConfigs);
  }

  public void registerSchemas() throws RestClientException, IOException {
    var stringSchema = Schema.create(Schema.Type.STRING);
    SchemaPublication schemaPublication = new SchemaPublication(schemaRegistryClient);

    schemaPublication.registerKeySchema(appConfig.getOrdersTopicName(), stringSchema);
    schemaPublication.registerValueSchema(appConfig.getOrdersTopicName(), Order.SCHEMA$);

    schemaPublication.registerKeySchema(appConfig.getOrdersNodesTopicName(), stringSchema);
    schemaPublication.registerValueSchema(
        appConfig.getOrdersNodesTopicName(), DomainGraphRecord.SCHEMA$);
  }
}
