package ps.hassany.kafka.tutorial.producer;

import lombok.Data;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;

@Data
public class Config {
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

  public static Config build(final String appProperties) throws IOException {
    var props = Config.loadProperties(appProperties);
    final Properties kafkaProducerProperties =
        Config.loadProperties(props.getProperty("kafkaProducerPropertiesFile"));
    final String outputTopic = kafkaProducerProperties.getProperty("output.topic.name");
    final String messageDelimiter = props.getProperty("messageDelimiter");
    final String defaultKey = props.getProperty("defaultKey");
    final String messagesFile = props.getProperty("messagesFile");

    final Path messageFilePath =
        Paths.get(
            Objects.requireNonNull(Config.class.getClassLoader().getResource(messagesFile))
                .getPath());
    return new Config(
        kafkaProducerProperties, outputTopic, messageFilePath, messageDelimiter, defaultKey);
  }
}
