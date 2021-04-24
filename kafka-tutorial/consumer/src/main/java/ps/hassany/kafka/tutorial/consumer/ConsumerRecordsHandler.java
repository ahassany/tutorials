package ps.hassany.kafka.tutorial.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

@FunctionalInterface
public interface ConsumerRecordsHandler<K, V, R> {
  R process(ConsumerRecords<K, V> consumerRecord);
}
