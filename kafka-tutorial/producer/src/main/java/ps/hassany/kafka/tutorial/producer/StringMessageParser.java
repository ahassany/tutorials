package ps.hassany.kafka.tutorial.producer;

import ps.hassany.kafka.tutorial.common.Message;

import java.util.Arrays;

public class StringMessageParser implements MessageLineParser<String, String> {
  private final String delimiter;
  private final String defaultKey;

  public StringMessageParser(String delimiter, String defaultKey) {
    this.delimiter = delimiter;
    this.defaultKey = defaultKey;
  }

  public Message<String, String> parseMessage(final String message) {
    String[] parts = message.split("-");
    String key, value;
    if (parts.length > 1) {
      key = parts[0];
      value = String.join(delimiter, Arrays.asList(parts).subList(1, parts.length));
    } else {
      key = defaultKey;
      value = message;
    }
    return new Message<>(key, value);
  }
}
