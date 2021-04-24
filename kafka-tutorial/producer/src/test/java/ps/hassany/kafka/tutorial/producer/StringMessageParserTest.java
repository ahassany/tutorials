package ps.hassany.kafka.tutorial.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ps.hassany.kafka.tutorial.common.Message;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringMessageParserTest {
  private ProducerApplicationConfig appConfig;

  @BeforeEach
  public void init() throws IOException {
    appConfig = ProducerApplicationConfig.build("app.properties");
  }

  @Test
  public void test_parseMessageWithKey() {
    final String key = "1";
    final String value = "value";
    final String message = key + appConfig.getMessageDelimiter() + value;
    Message<String, String> expected = new Message<>(key, value);
    var parser =
        new StringMessageParser(appConfig.getMessageDelimiter(), appConfig.getDefaultKey());
    var actual = parser.parseMessage(message);
    assertEquals(expected, actual);
  }

  @Test
  public void test_parseMessageWithoutKey() {
    final String key = "1";
    final String value = "value";
    final String message = key + value;
    Message<String, String> expected = new Message<>(appConfig.getDefaultKey(), message);
    var parser = new StringMessageParser(appConfig.getMessageDelimiter(), appConfig.getDefaultKey());
    var actual = parser.parseMessage(message);
    assertEquals(expected, actual);
  }
}
