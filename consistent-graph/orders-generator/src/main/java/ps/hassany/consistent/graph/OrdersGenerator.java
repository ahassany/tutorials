package ps.hassany.consistent.graph;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ps.hassany.consistent.graph.common.SchemaPublication;
import ps.hassany.consistent.graph.common.TopicCreation;
import ps.hassany.consistent.graph.common.TopicsCreationConfig;
import ps.hassany.consistent.graph.graph.Node;
import ps.hassany.consistent.graph.graph.Relation;
import ps.hassany.consistent.graph.orders.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

public class OrdersGenerator {

  private static final Logger logger = LoggerFactory.getLogger(OrdersGenerator.class);
  private final OrdersGeneratorAppConfig appConfig;
  private final AdminClient adminClient;
  private final CachedSchemaRegistryClient schemaRegistryClient;
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
      AdminClient adminClient,
      CachedSchemaRegistryClient schemaRegistryClient) {
    this.appConfig = appConfig;
    this.adminClient = adminClient;
    this.schemaRegistryClient = schemaRegistryClient;
  }

  public void createTopics() throws ExecutionException, InterruptedException {
    List<TopicsCreationConfig> topicConfigs =
        List.of(
            new TopicsCreationConfig(
                appConfig.getOrdersTopicName(),
                appConfig.getOrdersTopicPartitions(),
                appConfig.getOrdersTopicReplicationFactor()),
            new TopicsCreationConfig(
                appConfig.getOrdersNodesTopicName(),
                appConfig.getOrdersNodesTopicPartitions(),
                appConfig.getOrdersNodesTopicReplicationFactor(),
                Optional.of(Map.of("cleanup.policy", "compact"))),
            new TopicsCreationConfig(
                appConfig.getOrdersRelationsTopicName(),
                appConfig.getOrdersRelationsTopicPartitions(),
                appConfig.getOrdersRelationsTopicReplicationFactor(),
                Optional.of(Map.of("cleanup.policy", "compact"))));
    TopicCreation topicCreation = new TopicCreation(adminClient);
    topicCreation.createTopics(topicConfigs);
  }

  public void registerSchemas() throws RestClientException, IOException {

    var stringSchema = Schema.create(Schema.Type.STRING);
    SchemaPublication schemaPublication = new SchemaPublication(schemaRegistryClient);

    schemaPublication.registerKeySchema(appConfig.getOrdersTopicName(), stringSchema);
    schemaPublication.registerValueSchema(appConfig.getOrdersTopicName(), Order.SCHEMA$);

    schemaPublication.registerKeySchema(appConfig.getOrdersNodesTopicName(), stringSchema);
    schemaPublication.registerValueSchema(appConfig.getOrdersNodesTopicName(), Node.SCHEMA$);

    schemaPublication.registerKeySchema(appConfig.getOrdersRelationsTopicName(), stringSchema);
    schemaPublication.registerValueSchema(
        appConfig.getOrdersRelationsTopicName(), Relation.SCHEMA$);
  }

  public static Order newOrder(
      int customerRandomBound, int addressRandomBound, int bookRandomBound, int laptopRandomBound) {
    var random = new Random();
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
            .setIsbn("isbn" + bookNumber)
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
        .setOrderTimestamp(System.currentTimeMillis())
        .setCustomer(customer)
        .setOrderedItems(List.of(orderedItem1, orderedItem2))
        .build();
  }

  public void produce(int numOrders) {
    final KafkaProducer<String, Order> kafkaProducer =
        new KafkaProducer<>(appConfig.getKakfaProducerProperties());

    IntStream.range(0, numOrders + 1)
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
