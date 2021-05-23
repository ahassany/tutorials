package ps.hassany.consistent.graph.orders.stream.mapping;

import org.apache.kafka.streams.KeyValue;

import java.util.List;

public interface KeyValueMappingSupplier<K, V, KR, VR> {
  List<KeyValue<KR, VR>> map(K key, V value);
}
