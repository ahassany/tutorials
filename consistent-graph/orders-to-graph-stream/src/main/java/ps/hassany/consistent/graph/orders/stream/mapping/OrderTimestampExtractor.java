package ps.hassany.consistent.graph.orders.stream.mapping;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import ps.hassany.consistent.graph.orders.Order;

import java.time.Clock;

public class OrderTimestampExtractor implements TimestampExtractor {

  public static long getOrderTimestamp(Order order) {
    return order == null ? Clock.systemUTC().millis() : order.getOrderTimestamp();
  }

  @Override
  public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
    Order order = (Order) record.value();
    return getOrderTimestamp(order);
  }
}
