package ps.hassany.kafka.tutorial.kstreams.distinct.helpers;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import ps.hassany.kafka.tutorial.common.PropertiesClassPathLoader;
import ps.hassany.kafka.tutorial.common.TopicCreation;
import ps.hassany.kafka.tutorial.common.TopicsCreationConfig;
import ps.hassany.kafka.tutorial.kstreams.distinct.FindDisticntEventsApplicationConfig;

import java.util.List;
import java.util.Properties;

public class CreateTopics {

  public static void main(String[] args) throws Exception {
    Properties properties = PropertiesClassPathLoader.loadProperties("dev.properties");
    FindDisticntEventsApplicationConfig config =
        FindDisticntEventsApplicationConfig.build(properties);
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
                config.getOutputTopicName(),
                config.getOutputTopicPartitions(),
                config.getOutputTopicReplicationFactor()));
    TopicCreation topicCreation = new TopicCreation(adminClient);
    topicCreation.createTopics(topicConfigs);
  }
}
