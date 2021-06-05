package ps.hassany.consistent.graph.orders.stream;

import lombok.Data;
import ps.hassany.consistent.graph.common.PropertiesClassPathLoader;

import java.io.IOException;
import java.util.Properties;

@Data
public class OrdersStreamingAppConfig {
  private final Properties kakfaProducerProperties;
  private final String bootstrapServers;
  private final String applicationId;
  private final String schemaRegistryURL;

  private final String ordersTopicName;
  private final int ordersTopicPartitions;
  private final short ordersTopicReplicationFactor;

  private final String ordersNodesTopicName;
  private final int ordersNodesTopicPartitions;
  private final short ordersNodesTopicReplicationFactor;

  private final String ordersDLQTopicName;
  private final int ordersDLQTopicPartitions;
  private final short ordersDLQTopicReplicationFactor;

  public static OrdersStreamingAppConfig build(Properties props) throws IOException {
    final Properties kafkaProducerProperties =
        PropertiesClassPathLoader.loadProperties(
            props.getProperty("kafka.producer.properties.file"));

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

    final String ordersDLQTopicName = props.getProperty("orders.dql.topic.name");
    final int ordersDLQTopicPartitions =
        Integer.parseInt(props.getProperty("orders.dql.topic.partitions"));
    final short ordersDLQTopicReplicationFactor =
        Short.parseShort(props.getProperty("orders.dql.topic.replication.factor"));

    return new OrdersStreamingAppConfig(
        kafkaProducerProperties,
        bootstrapServers,
        applicationId,
        schemaRegistryURL,
        ordersTopicName,
        ordersTopicPartitions,
        ordersTopicReplicationFactor,
        ordersNodesTopicName,
        ordersNodesTopicPartitions,
        ordersNodesTopicReplicationFactor,
        ordersDLQTopicName,
        ordersDLQTopicPartitions,
        ordersDLQTopicReplicationFactor);
  }
}
