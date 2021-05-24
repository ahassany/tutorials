package ps.hassany.consistent.graph.orders.stream.mapping;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import ps.hassany.consistent.graph.orders.Order;

public class OrderTimestampExtractor implements TimestampExtractor {

  @Override
  public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
    return ((Order) record.value()).getOrderTimestamp();
  }
}
