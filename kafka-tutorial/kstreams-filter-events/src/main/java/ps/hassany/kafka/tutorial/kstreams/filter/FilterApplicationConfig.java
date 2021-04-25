package ps.hassany.kafka.tutorial.kstreams.filter;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Properties;

@Data
@AllArgsConstructor
public class FilterApplicationConfig {
  private final String bootstrapServers;
  private final String applicationId;
  private final String schemaRegistryURL;

  private final String inputTopicName;
  private final int inputTopicPartitions;
  private final short inputTopicReplicationFactor;
  private final String outputTopicName;
  private final int outputTopicPartitions;
  private final short outputTopicReplicationFactor;

  public static FilterApplicationConfig build(Properties props) {
    final String bootstrapServers = props.getProperty("bootstrap.servers");
    final String schemaRegistryURL = props.getProperty("schema.registry.url");
    final String applicationId = props.getProperty("application.id");

    final String inputTopicName = props.getProperty("input.topic.name");
    final int inputTopicPartitions = Integer.parseInt(props.getProperty("input.topic.partitions"));
    final short inputTopicReplicationFactor =
        Short.parseShort(props.getProperty("input.topic.replication.factor"));

    final String outputTopicName = props.getProperty("output.topic.name");
    final int outputTopicPartitions =
        Short.parseShort(props.getProperty("output.topic.partitions"));
    final short outputTopicReplicationFactor =
        Short.parseShort(props.getProperty("output.topic.replication.factor"));
    return new FilterApplicationConfig(
        bootstrapServers,
        applicationId,
        schemaRegistryURL,
        inputTopicName,
        inputTopicPartitions,
        inputTopicReplicationFactor,
        outputTopicName,
        outputTopicPartitions,
        outputTopicReplicationFactor);
  }
}
