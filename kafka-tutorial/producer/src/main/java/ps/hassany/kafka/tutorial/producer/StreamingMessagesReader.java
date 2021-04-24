package ps.hassany.kafka.tutorial.producer;

import ps.hassany.kafka.tutorial.common.Message;

import java.io.IOException;
import java.util.stream.Stream;

public interface StreamingMessagesReader<K, V> {
  Stream<Message<K, V>> streamMessages() throws IOException;
}
