package ps.hassany.consistent.graph.orders.stream;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import ps.hassany.consistent.graph.common.PropertiesClassPathLoader;
import ps.hassany.consistent.graph.common.SchemaPublication;
import ps.hassany.consistent.graph.common.TopicCreation;
import ps.hassany.consistent.graph.common.TopicsCreationConfig;
import ps.hassany.consistent.graph.orders.internal.DLQRecord;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TopicsManagement {

  private static void registerSchemas(
      CachedSchemaRegistryClient schemaRegistryClient,
      String topicName,
      Schema keySchema,
      Schema valueSchema)
      throws RestClientException, IOException {
    SchemaPublication schemaPublication = new SchemaPublication(schemaRegistryClient);
    schemaPublication.registerKeySchema(topicName, keySchema);
    schemaPublication.registerValueSchema(topicName, valueSchema);
  }

  private static void createTopic(
      AdminClient adminClient, String topicName, int topicPartitions, short topicReplicas)
      throws ExecutionException, InterruptedException {
    List<TopicsCreationConfig> topicConfigs =
        List.of(new TopicsCreationConfig(topicName, topicPartitions, topicReplicas));
    TopicCreation topicCreation = new TopicCreation(adminClient);
    topicCreation.createTopics(topicConfigs);
  }

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException, RestClientException {
    Properties props = PropertiesClassPathLoader.loadProperties("dev.properties");
    OrdersStreamingAppConfig appConfig = OrdersStreamingAppConfig.build(props);
    Properties adminClientProps = new Properties();
    adminClientProps.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
    AdminClient adminClient = AdminClient.create(adminClientProps);
    TopicsManagement.createTopic(
        adminClient,
        appConfig.getOrdersDLQTopicName(),
        appConfig.getOrdersDLQTopicPartitions(),
        appConfig.getOrdersDLQTopicReplicationFactor());

    CachedSchemaRegistryClient schemaRegistryClient =
        new CachedSchemaRegistryClient(appConfig.getSchemaRegistryURL(), 10);

    var stringSchema = Schema.create(Schema.Type.STRING);
    registerSchemas(
        schemaRegistryClient, appConfig.getOrdersDLQTopicName(), stringSchema, DLQRecord.SCHEMA$);
  }
}
