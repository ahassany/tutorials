package ps.hassany.kafka.tutorial.producer;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringMessageParserTest {

  @Test
  public void test_parseMessageWithKey() {
    final String delimiter = "-";
    final String defaultKey = "defaultKey";
    final String key = "1";
    final String value = "value";
    final String message = key + delimiter + value;
    Message<String, String> expected = new Message<>(key, value);
    var parser = new StringMessageParser(delimiter, defaultKey);
    var actual = parser.parseMessage(message);
    assertEquals(expected, actual);
  }

  @Test
  public void test_parseMessageWithoutKey() {
    final String delimiter = "-";
    final String defaultKey = "defaultKey";
    final String key = "1";
    final String value = "value";
    final String message = key + value;
    Message<String, String> expected = new Message<>(defaultKey, message);
    var parser = new StringMessageParser(delimiter, defaultKey);
    var actual = parser.parseMessage(message);
    assertEquals(expected, actual);
  }
}
