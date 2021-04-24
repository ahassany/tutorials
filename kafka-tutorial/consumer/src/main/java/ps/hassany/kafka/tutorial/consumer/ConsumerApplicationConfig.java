package ps.hassany.kafka.tutorial.consumer;

import lombok.Data;
import ps.hassany.kafka.tutorial.common.PropertiesClassPathLoader;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

@Data
public class ConsumerApplicationConfig {
  private final Properties kafkaConsumerProperties;
  private final String inputTopic;
  private final Duration pollTimeoutDuration;

  public static ConsumerApplicationConfig build(final String appPropertiesFile) throws IOException {
    var appProperties = PropertiesClassPathLoader.loadProperties(appPropertiesFile);
    final Properties kafkaConsumerProperties =
        PropertiesClassPathLoader.loadProperties(
            appProperties.getProperty("kafka.consumer.properties.file"));
    final String inputTopic = appProperties.getProperty("input.topic.name");
    final Duration pollTimeout =
        Duration.ofSeconds(Long.parseLong(appProperties.getProperty("poll.timeout.seconds")));
    return new ConsumerApplicationConfig(kafkaConsumerProperties, inputTopic, pollTimeout);
  }
}
