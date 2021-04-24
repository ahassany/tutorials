package ps.hassany.kafka.tutorial.producer;

import ps.hassany.kafka.tutorial.common.Message;

import java.io.IOException;

public interface MessageLineParser<K, V> {
  Message<K, V> parseMessage(final String message) throws IOException;
}
