package ps.hassany.kafka.tutorial.producer;

import java.io.IOException;
import java.util.stream.Stream;

public interface StreamingMessagesReader<K, V> {
  Stream<Message<K, V>> streamMessages() throws IOException;
}
