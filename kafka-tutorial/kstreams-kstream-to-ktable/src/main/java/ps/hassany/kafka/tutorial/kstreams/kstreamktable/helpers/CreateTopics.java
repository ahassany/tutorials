package ps.hassany.kafka.tutorial.kstreams.kstreamktable.helpers;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import ps.hassany.kafka.tutorial.common.PropertiesClassPathLoader;
import ps.hassany.kafka.tutorial.common.TopicCreation;
import ps.hassany.kafka.tutorial.common.TopicsCreationConfig;
import ps.hassany.kafka.tutorial.kstreams.kstreamktable.KStreamToKTableConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class CreateTopics {
  public static void main(String[] args) throws Exception {
    Properties properties = PropertiesClassPathLoader.loadProperties("dev.properties");
    KStreamToKTableConfig config = KStreamToKTableConfig.build(properties);
    Properties adminClientProps = new Properties();
    adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
    AdminClient adminClient = AdminClient.create(adminClientProps);

    List<TopicsCreationConfig> topicConfigs =
        List.of(
            new TopicsCreationConfig(
                config.getInputTopicName(),
                config.getInputTopicPartitions(),
                config.getInputTopicReplicationFactor()),
            new TopicsCreationConfig(
                config.getStreamsOutputTopicName(),
                config.getStreamsOutputTopicPartitions(),
                config.getStreamsOutputTopicReplicationFactor()),
            new TopicsCreationConfig(
                config.getTableOutputTopicName(),
                config.getTableOutputTopicPartitions(),
                config.getTableOutputTopicReplicationFactor(),
                Optional.of(
                    Map.of(
                        "cleanup.policy",
                        "compact",
                        "retention.ms",
                        "100",
                        "delete.retention.ms",
                        "100",
                        "min.cleanable.dirty.ratio",
                        "0.000001",
                        "min.compaction.lag.ms",
                        "0"))));
    TopicCreation topicCreation = new TopicCreation(adminClient);
    topicCreation.createTopics(topicConfigs);
  }
}
