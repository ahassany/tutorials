package ps.hassany.kafka.tutorial.producer;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class Message<K, V> {
  final K key;
  final V value;
}
