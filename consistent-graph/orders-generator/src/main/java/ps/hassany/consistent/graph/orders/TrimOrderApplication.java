package ps.hassany.consistent.graph.orders;

import gr.james.sampling.ChaoSampling;
import gr.james.sampling.RandomSampling;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ps.hassany.consistent.graph.common.PropertiesClassPathLoader;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class TrimOrderApplication {
  private static final Logger logger = LoggerFactory.getLogger(TrimOrderApplication.class);

  public static Future<RecordMetadata> updateOrder(
      final Producer<String, Order> producer, String topicName, final Order order) {
    logger.info("Updating Order order: " + order.getId());
    return producer.send(new ProducerRecord<>(topicName, order.getId(), order));
  }

  public static Order deleteItemsFromOrder(final Order order, final int k, final Random random) {
    RandomSampling<OrderedItem> rs = new ChaoSampling<>(k, random);
    rs.feed(order.getOrderedItems().iterator());
    final var sampledOrderItems = rs.sample();
    sampledOrderItems.forEach(
        x -> logger.info("Going to delete " + order.getId() + " item: " + x.getDetails()));
    order.setOrderedItems(
        order.getOrderedItems().stream()
            .filter(x -> !sampledOrderItems.contains(x))
            .collect(Collectors.toList()));
    return order;
  }

  public static void main(String[] args) throws Exception {
    // Default 1 order
    int numOrders = 1;
    // Default Items
    int numItems = 1;
    if (args.length > 0) {
      numOrders = Integer.parseInt(args[0]);
    }
    if (args.length > 1) {
      numItems = Integer.parseInt(args[1]);
    }
    logger.info(String.format("Deleting %s orders", numOrders));
    Properties properties = PropertiesClassPathLoader.loadProperties("dev.properties");
    OrdersGeneratorAppConfig appConfig = OrdersGeneratorAppConfig.build(properties);

    Consumer<String, Order> consumer = new KafkaConsumer<>(appConfig.getKakfaConsumerProperties());
    consumer.subscribe(List.of(appConfig.getOrdersTopicName()));
    OrdersConsumer ordersConsumer = new OrdersConsumer(appConfig, consumer);

    Producer<String, Order> producer = new KafkaProducer<>(appConfig.getKakfaProducerProperties());

    Random random = new Random();
    RandomSampling<Order> rs = new ChaoSampling<>(numOrders, random);
    rs.feed(
        ordersConsumer
            .poll()
            .map(consumerRecord -> consumerRecord.value())
            .filter(Objects::nonNull)
            .iterator());
    var sampledOrders = rs.sample();
    int finalNumItems = numItems;
    sampledOrders.stream()
        .map(x -> deleteItemsFromOrder(x, finalNumItems, random))
        .map(order -> updateOrder(producer, appConfig.getOrdersTopicName(), order))
        .forEach(
            x -> {
              try {
                x.get();
              } catch (Exception ex) {
                logger.error("ERROR", ex);
              }
            });
  }
}
