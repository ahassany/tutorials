package ps.hassany.kafka.tutorial.kstreams.split.helpers;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ps.hassany.kafka.tutorial.common.PropertiesClassPathLoader;
import ps.hassany.kafka.tutorial.common.TopicCreation;
import ps.hassany.kafka.tutorial.common.TopicsCreationConfig;
import ps.hassany.kafka.tutorial.kstreams.split.SplitStreamConfig;

import java.util.List;
import java.util.Properties;

public class CreateTopics {
  private static final Logger logger = LoggerFactory.getLogger(CreateTopics.class);

  public static void main(String[] args) throws Exception {
    Properties properties = PropertiesClassPathLoader.loadProperties("dev.properties");
    SplitStreamConfig config = SplitStreamConfig.build(properties);
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
                config.getDramaTopicName(),
                config.getDramaTopicPartitions(),
                config.getDramaTopicReplicationFactor()),
            new TopicsCreationConfig(
                config.getFantasyTopicName(),
                config.getFantasyTopicPartitions(),
                config.getFantasyTopicReplicationFactor()),
            new TopicsCreationConfig(
                config.getOtherTopicName(),
                config.getOtherTopicPartitions(),
                config.getOtherTopicReplicationFactor()));
    TopicCreation topicCreation = new TopicCreation(adminClient);
    topicCreation.createTopics(config.getBootstrapServers(), topicConfigs);
  }
}
