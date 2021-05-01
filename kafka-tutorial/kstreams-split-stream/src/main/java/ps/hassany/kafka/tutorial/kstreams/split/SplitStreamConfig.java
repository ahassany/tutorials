package ps.hassany.kafka.tutorial.kstreams.split;

import lombok.Data;

import java.util.Properties;

@Data
public class SplitStreamConfig {
  private final String bootstrapServers;
  private final String applicationId;
  private final String schemaRegistryURL;

  private final String inputTopicName;
  private final int inputTopicPartitions;
  private final short inputTopicReplicationFactor;

  private final String dramaTopicName;
  private final int dramaTopicPartitions;
  private final short dramaTopicReplicationFactor;

  private final String fantasyTopicName;
  private final int fantasyTopicPartitions;
  private final short fantasyTopicReplicationFactor;

  private final String otherTopicName;
  private final int otherTopicPartitions;
  private final short otherTopicReplicationFactor;

  public static SplitStreamConfig build(Properties props) {
    final String bootstrapServers = props.getProperty("bootstrap.servers");
    final String schemaRegistryURL = props.getProperty("schema.registry.url");
    final String applicationId = props.getProperty("application.id");

    final String inputTopicName = props.getProperty("input.topic.name");
    final int inputTopicPartitions = Integer.parseInt(props.getProperty("input.topic.partitions"));
    final short inputTopicReplicationFactor =
        Short.parseShort(props.getProperty("input.topic.replication.factor"));

    final String dramaTopicName = props.getProperty("output.drama.topic.name");
    final int dramaTopicPartitions =
        Integer.parseInt(props.getProperty("output.drama.topic.partitions"));
    final short dramaTopicReplicationFactor =
        Short.parseShort(props.getProperty("output.drama.topic.replication.factor"));

    final String fantasyTopicName = props.getProperty("output.fantasy.topic.name");
    final int fantasyTopicPartitions =
        Integer.parseInt(props.getProperty("output.fantasy.topic.partitions"));
    final short fantasyTopicReplicationFactor =
        Short.parseShort(props.getProperty("output.fantasy.topic.replication.factor"));

    final String otherTopicName = props.getProperty("output.other.topic.name");
    final int otherTopicPartitions =
        Integer.parseInt(props.getProperty("output.other.topic.partitions"));
    final short otherTopicReplicationFactor =
        Short.parseShort(props.getProperty("output.other.topic.replication.factor"));

    return new SplitStreamConfig(
        bootstrapServers,
        applicationId,
        schemaRegistryURL,
        inputTopicName,
        inputTopicPartitions,
        inputTopicReplicationFactor,
        dramaTopicName,
        dramaTopicPartitions,
        dramaTopicReplicationFactor,
        fantasyTopicName,
        fantasyTopicPartitions,
        fantasyTopicReplicationFactor,
        otherTopicName,
        otherTopicPartitions,
        otherTopicReplicationFactor);
  }
}
