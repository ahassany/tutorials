package ps.hassany.consistent.graph.orders;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class OrdersConsumer {
  private static final Logger logger = LoggerFactory.getLogger(OrdersConsumer.class);
  private final Consumer<String, Order> consumer;
  private final OrdersGeneratorAppConfig appConfig;

  public OrdersConsumer(
      final OrdersGeneratorAppConfig appConfig, final Consumer<String, Order> consumer) {
    this.appConfig = appConfig;
    this.consumer = consumer;
  }

  public Stream<ConsumerRecord<String, Order>> poll() {
    var x = consumer.poll(Duration.ofSeconds(1));

    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(x.iterator(), Spliterator.ORDERED), false);
  }
}
