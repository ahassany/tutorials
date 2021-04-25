package ps.hassany.kafka.tutorial.kstreams.filter;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import ps.hassany.kafka.tutorial.common.PropertiesClassPathLoader;

import java.util.*;

public class FilterEventsApplication {

  public static void createTopics(FilterApplicationConfig applicationConfig) {
    Map<String, Object> config = new HashMap<>();
    config.put("bootstrap.servers", applicationConfig.getBootstrapServers());
    AdminClient client = AdminClient.create(config);

    List<NewTopic> topics = new ArrayList<>();
    topics.add(new NewTopic(
            applicationConfig.getInputTopicName(),
            applicationConfig.getInputTopicPartitions(),
            applicationConfig.getInputTopicReplicationFactor()));
    topics.add(new NewTopic(
            applicationConfig.getOutputTopicName(),
            applicationConfig.getInputTopicPartitions(),
            applicationConfig.getOutputTopicReplicationFactor()));

    client.createTopics(topics);
    client.close();
  }


  public static void main(String[] args) {
    final Properties appProperties;
    try {
      appProperties = PropertiesClassPathLoader.loadProperties("app.properties");
    } catch (Exception exception) {
      System.err.println("Error loading application properties: " + exception.getMessage());
      exception.printStackTrace();
      return;
    }

    FilterApplicationConfig appConfig = FilterApplicationConfig.build(appProperties);
    FilterEvents filterEvents = new FilterEvents(appConfig);
    FilterEventsApplication.createTopics(appConfig);
    filterEvents.runRecipe();
  }
}
