package ps.hassany.kafka.tutorial.kstreams.kstreamktable;

import lombok.Data;
import ps.hassany.kafka.tutorial.common.PropertiesClassPathLoader;
import ps.hassany.kafka.tutorial.producer.ProducerApplicationConfig;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

@Data
public class KStreamToKTableConfig {
  private final Properties kakfaProducerProperties;
  private final String bootstrapServers;
  private final String applicationId;
  private final String schemaRegistryURL;
  private final Path messagesFilePath;

  private final String inputTopicName;
  private final int inputTopicPartitions;
  private final short inputTopicReplicationFactor;

  private final String streamsOutputTopicName;
  private final int streamsOutputTopicPartitions;
  private final short streamsOutputTopicReplicationFactor;

  private final String tableOutputTopicName;
  private final int tableOutputTopicPartitions;
  private final short tableOutputTopicReplicationFactor;

  public static KStreamToKTableConfig build(Properties props) throws Exception {
    final Properties kafkaProducerProperties =
        PropertiesClassPathLoader.loadProperties(
            props.getProperty("kafka.producer.properties.file"));
    final String bootstrapServers = props.getProperty("bootstrap.servers");
    final String schemaRegistryURL = props.getProperty("schema.registry.url");
    final String applicationId = props.getProperty("application.id");
    final String messagesFile = props.getProperty("messages.file");

    final Path messageFilePath =
        Paths.get(
            Objects.requireNonNull(
                    ProducerApplicationConfig.class.getClassLoader().getResource(messagesFile))
                .getPath());

    final String inputTopicName = props.getProperty("input.topic.name");
    final int inputTopicPartitions = Integer.parseInt(props.getProperty("input.topic.partitions"));
    final short inputTopicReplicationFactor =
        Short.parseShort(props.getProperty("input.topic.replication.factor"));

    final String streamsOutputTopicName = props.getProperty("streams.output.topic.name");
    final int streamsOutputTopicPartitions =
        Integer.parseInt(props.getProperty("streams.output.topic.partitions"));
    final short streamsOutputTopicReplicationFactor =
        Short.parseShort(props.getProperty("streams.output.topic.replication.factor"));

    final String tableOutputTopicName = props.getProperty("table.output.topic.name");
    final int tableOutputTopicPartitions =
        Integer.parseInt(props.getProperty("table.output.topic.partitions"));
    final short tableOutputTopicReplicationFactor =
        Short.parseShort(props.getProperty("table.output.topic.replication.factor"));

    return new KStreamToKTableConfig(
        kafkaProducerProperties,
        bootstrapServers,
        applicationId,
        schemaRegistryURL,
        messageFilePath,
        inputTopicName,
        inputTopicPartitions,
        inputTopicReplicationFactor,
        streamsOutputTopicName,
        streamsOutputTopicPartitions,
        streamsOutputTopicReplicationFactor,
        tableOutputTopicName,
        tableOutputTopicPartitions,
        tableOutputTopicReplicationFactor);
  }
}
