package ps.hassany.kafka.tutorial.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesClassPathLoader {
  public static Properties loadProperties(final String propertiesFilename) throws IOException {
    final Properties properties = new Properties();
    var configURL =
        PropertiesClassPathLoader.class.getClassLoader().getResource(propertiesFilename);
    assert configURL != null;
    try (InputStream inputStream = configURL.openStream()) {
      properties.load(inputStream);
    }
    return properties;
  }
}
