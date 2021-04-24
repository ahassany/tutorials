package ps.hassany.kafka.tutorial.common;

import lombok.Data;

@Data
public class Message<K, V> {
  private final K key;
  private final V value;
}
