package ps.hassany.kafka.tutorial.producer;

import lombok.Data;

import java.io.IOException;
import java.io.InputStream;
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

  public static Properties loadProperties(final String propertiesFilename) throws IOException {
    final Properties properties = new Properties();
    var configURL = KafkaProducerApplication.class.getClassLoader().getResource(propertiesFilename);
    assert configURL != null;
    try (InputStream inputStream = configURL.openStream()) {
      properties.load(inputStream);
    }
    return properties;
  }

  public static ProducerApplicationConfig build(final String appProperties) throws IOException {
    var applicationProperties = ProducerApplicationConfig.loadProperties(appProperties);
    final Properties kafkaProducerProperties =
        ProducerApplicationConfig.loadProperties(
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
