package ps.hassany.consistent.graph.orders;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class OrdersGenerator {

  private static final Logger logger = LoggerFactory.getLogger(OrdersGenerator.class);
  private final OrdersGeneratorAppConfig appConfig;
  private final Producer<String, Order> kafkaProducer;
  private final Random random;
  private final Callback printCallback =
      (recordMetadata, exception) -> {
        if (exception != null) {
          logger.error("Exception: " + exception.getMessage(), exception);
          // exception.printStackTrace(System.err);
        } else {
          logger.info(
              String.format(
                  "Kafka Producer Callback: Partition: %d, Offset: %d, Exception: None",
                  recordMetadata.partition(), recordMetadata.offset()));
        }
      };

  public OrdersGenerator(
      OrdersGeneratorAppConfig appConfig,
      final Producer<String, Order> kafkaProducer,
      final Random random) {
    this.appConfig = appConfig;
    this.kafkaProducer = kafkaProducer;
    this.random = random;
  }

  public Order newOrder(
      int customerRandomBound, int addressRandomBound, int bookRandomBound, int laptopRandomBound) {
    var bookBuilder = BookOrder.newBuilder();
    var laptopBuilder = LaptopOrder.newBuilder();
    var orderItemsBuilder = OrderedItem.newBuilder();
    var orderBuilder = Order.newBuilder();
    var customerBuilder = CustomerOrder.newBuilder();

    var customerNumber = random.nextInt(customerRandomBound);
    var addressNumber = random.nextInt(addressRandomBound);
    var bookNumber = random.nextInt(bookRandomBound);
    var laptopNumber = random.nextInt(laptopRandomBound);

    var customer =
        customerBuilder
            .setName("Customer" + customerNumber)
            .setAddress("Address " + addressNumber)
            .build();

    var book =
        bookBuilder
            .setId("book" + bookNumber)
            .setName("Book " + bookNumber)
            .setIsbn("isbn:" + bookNumber)
            .build();

    var laptop =
        laptopBuilder.setId("laptop" + laptopNumber).setName("Laptop " + laptopNumber).build();

    var orderedItem1 =
        orderItemsBuilder
            .setItemType(OrderedItemType.book)
            .setPrice(random.nextInt(100))
            .setDetails(book)
            .build();
    var orderedItem2 =
        orderItemsBuilder
            .setItemType(OrderedItemType.laptop)
            .setPrice(random.nextInt(3000))
            .setDetails(laptop)
            .build();

    return orderBuilder
        .setId(UUID.randomUUID().toString())
        .setOrderTimestamp(Clock.systemDefaultZone().millis())
        .setCustomer(customer)
        .setOrderedItems(List.of(orderedItem1, orderedItem2))
        .build();
  }

  public void produce(int numOrders) {
    IntStream.range(0, numOrders)
        .mapToObj(
            x -> {
              var order = newOrder(10, 100, 10, 2);
              return kafkaProducer.send(
                  new ProducerRecord<>(appConfig.getOrdersTopicName(), order.getId(), order),
                  printCallback);
            })
        .parallel()
        .forEach(
            f -> {
              try {
                f.get();
              } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
              }
            });
    kafkaProducer.close();
  }
}
