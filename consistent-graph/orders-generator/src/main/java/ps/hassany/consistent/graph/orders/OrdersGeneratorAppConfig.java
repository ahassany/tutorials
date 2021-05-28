package ps.hassany.consistent.graph.orders;

import lombok.Data;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import ps.hassany.consistent.graph.common.PropertiesClassPathLoader;

import java.io.IOException;
import java.util.Properties;

@Data
public class OrdersGeneratorAppConfig {
  private final Properties kakfaProducerProperties;
  private final Properties kakfaConsumerProperties;
  private final String bootstrapServers;
  private final String applicationId;
  private final String schemaRegistryURL;

  private final String ordersTopicName;
  private final int ordersTopicPartitions;
  private final short ordersTopicReplicationFactor;

  private final String ordersNodesTopicName;
  private final int ordersNodesTopicPartitions;
  private final short ordersNodesTopicReplicationFactor;

  private final String ordersRelationsTopicName;
  private final int ordersRelationsTopicPartitions;
  private final short ordersRelationsTopicReplicationFactor;

  public static OrdersGeneratorAppConfig build(Properties props) throws IOException {
    final Properties kafkaProducerProperties =
        PropertiesClassPathLoader.loadProperties(
            props.getProperty("kafka.producer.properties.file"));
    final Properties KafkaConsumerProperties =
        PropertiesClassPathLoader.loadProperties(
            props.getProperty("kafka.consumer.properties.file"));

    KafkaConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "orders.delete");
    KafkaConsumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "orders.delete.client");
    KafkaConsumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    final String bootstrapServers = props.getProperty("bootstrap.servers");
    final String schemaRegistryURL = props.getProperty("schema.registry.url");
    final String applicationId = props.getProperty("application.id");

    final String ordersTopicName = props.getProperty("orders.topic.name");
    final int ordersTopicPartitions =
        Integer.parseInt(props.getProperty("orders.topic.partitions"));
    final short ordersTopicReplicationFactor =
        Short.parseShort(props.getProperty("orders.topic.replication.factor"));

    final String ordersNodesTopicName = props.getProperty("orders.nodes.topic.name");
    final int ordersNodesTopicPartitions =
        Integer.parseInt(props.getProperty("orders.nodes.topic.partitions"));
    final short ordersNodesTopicReplicationFactor =
        Short.parseShort(props.getProperty("orders.nodes.topic.replication.factor"));

    final String ordersRelationsTopicName = props.getProperty("orders.relations.topic.name");
    final int ordersRelationsTopicPartitions =
        Integer.parseInt(props.getProperty("orders.relations.topic.partitions"));
    final short ordersRelationsTopicReplicationFactor =
        Short.parseShort(props.getProperty("orders.relations.topic.replication.factor"));

    return new OrdersGeneratorAppConfig(
        kafkaProducerProperties,
        KafkaConsumerProperties,
        bootstrapServers,
        applicationId,
        schemaRegistryURL,
        ordersTopicName,
        ordersTopicPartitions,
        ordersTopicReplicationFactor,
        ordersNodesTopicName,
        ordersNodesTopicPartitions,
        ordersNodesTopicReplicationFactor,
        ordersRelationsTopicName,
        ordersRelationsTopicPartitions,
        ordersRelationsTopicReplicationFactor);
  }
}
