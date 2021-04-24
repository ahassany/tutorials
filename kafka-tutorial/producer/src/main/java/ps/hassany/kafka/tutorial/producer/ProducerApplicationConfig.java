package ps.hassany.kafka.tutorial.producer;

import lombok.Data;
import ps.hassany.kafka.tutorial.common.PropertiesClassPathLoader;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

@Data
public class ProducerApplicationConfig {
  private final Properties kakfaProducerProperties;
  private final String outTopic;
  private final Path messagesFilePath;
  private final String messageDelimiter;
  private final String defaultKey;

  public static ProducerApplicationConfig build(final String appProperties) throws IOException {
    var applicationProperties = PropertiesClassPathLoader.loadProperties(appProperties);
    final Properties kafkaProducerProperties =
        PropertiesClassPathLoader.loadProperties(
            applicationProperties.getProperty("kafka.producer.properties.file"));
    final String outputTopic = applicationProperties.getProperty("output.topic.name");
    final String messageDelimiter = applicationProperties.getProperty("message.delimiter");
    final String defaultKey = applicationProperties.getProperty("default.key");
    final String messagesFile = applicationProperties.getProperty("messages.file");

    final Path messageFilePath =
        Paths.get(
            Objects.requireNonNull(
                    ProducerApplicationConfig.class.getClassLoader().getResource(messagesFile))
                .getPath());
    return new ProducerApplicationConfig(
        kafkaProducerProperties, outputTopic, messageFilePath, messageDelimiter, defaultKey);
  }
}
