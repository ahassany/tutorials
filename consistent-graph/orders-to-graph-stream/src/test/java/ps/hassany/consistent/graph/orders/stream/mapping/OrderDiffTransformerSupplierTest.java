package ps.hassany.consistent.graph.orders.stream.mapping;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ps.hassany.consistent.graph.common.PropertiesClassPathLoader;
import ps.hassany.consistent.graph.domain.DomainNode;
import ps.hassany.consistent.graph.orders.Order;
import ps.hassany.consistent.graph.orders.internal.OrderWithState;
import ps.hassany.consistent.graph.orders.stream.OrderSerdes;
import ps.hassany.consistent.graph.orders.stream.OrdersStreamingAppConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OrderDiffTransformerSupplierTest {
  private static final String TEST_CONFIG_FILE = "test.properties";
  private final String outTopicName = "output-topic";
  private static final Path STATE_DIR = Paths.get(System.getProperty("user.dir"), "build");
  private static final String storeName = "test-store";
  private OrdersStreamingAppConfig appConfig;
  private Serde<String> stringSerde;
  private Serde<Order> orderSerde;
  private Serde<DomainNode> nodeSerde;
  private Serde<OrderWithState> orderWithStateSerde;
  private StreamsBuilder builder;

  private Properties buildStreamsProperties(final OrdersStreamingAppConfig config) {
    Properties streamConfig = new Properties();
    streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getApplicationId());
    streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
    streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    streamConfig.put(SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryURL());
    streamConfig.put(
        StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        OrderTimestampExtractor.class.getName());
    streamConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    return streamConfig;
  }

  @BeforeEach
  public void init() throws IOException {
    Properties props = PropertiesClassPathLoader.loadProperties(TEST_CONFIG_FILE);
    appConfig = OrdersStreamingAppConfig.build(props);
    stringSerde = Serdes.String();
    orderSerde = OrderSerdes.ordersSerde(appConfig);
    nodeSerde = OrderSerdes.nodeSerde(appConfig);
    orderWithStateSerde = OrderSerdes.orderWithStateSerde(appConfig);

    final StoreBuilder<WindowStore<String, Order>> ordersStoreBuilder =
        Stores.windowStoreBuilder(
            Stores.persistentWindowStore(
                storeName, Duration.ofSeconds(100), Duration.ofSeconds(100), false),
            stringSerde,
            orderSerde);
    final var supplier = new OrderDiffTransformerSupplier(storeName, 100000, ordersStoreBuilder);
    builder = new StreamsBuilder();
    final KStream<String, Order> ordersStream = builder.stream(appConfig.getOrdersTopicName());
    final KStream<String, OrderWithState> orderWithStateKStream = ordersStream.transform(supplier);
    orderWithStateKStream.to(outTopicName, Produced.with(stringSerde, orderWithStateSerde));
  }

  @Test
  public void test_transformOneOrder() {
    long orderTimestamp = Clock.systemUTC().millis();
    var order1 = TestUtils.getOrder1(orderTimestamp);
    try (final TopologyTestDriver topologyTestDriver =
        new TopologyTestDriver(builder.build(), buildStreamsProperties(appConfig))) {
      final TestInputTopic<String, Order> input =
          topologyTestDriver.createInputTopic(
              appConfig.getOrdersTopicName(), stringSerde.serializer(), orderSerde.serializer());

      final TestOutputTopic<String, OrderWithState> output =
          topologyTestDriver.createOutputTopic(
              outTopicName, stringSerde.deserializer(), orderWithStateSerde.deserializer());
      input.pipeInput(order1.getId(), order1);
      var ret = output.readKeyValuesToList();
      var orderWithState = TestUtils.getOrder1CreateState(orderTimestamp);
      assertEquals(List.of(new KeyValue<>(orderWithState.getId(), orderWithState)), ret);
    }
  }

  @Test
  public void test_transformTwoOrders() {
    long order1Timestamp = Clock.systemUTC().millis() - 1000;
    long order2Timestamp = Clock.systemUTC().millis();
    var order1 = TestUtils.getOrder1(order1Timestamp);
    var order2 = TestUtils.getOrder2(order2Timestamp);
    try (final TopologyTestDriver topologyTestDriver =
        new TopologyTestDriver(builder.build(), buildStreamsProperties(appConfig))) {
      final TestInputTopic<String, Order> input =
          topologyTestDriver.createInputTopic(
              appConfig.getOrdersTopicName(), stringSerde.serializer(), orderSerde.serializer());

      final TestOutputTopic<String, OrderWithState> output =
          topologyTestDriver.createOutputTopic(
              outTopicName, stringSerde.deserializer(), orderWithStateSerde.deserializer());
      input.pipeInput(order1.getId(), order1);
      input.pipeInput(order2.getId(), order2);
      var ret = output.readKeyValuesToList();
      var order1WithState = TestUtils.getOrder1CreateState(order1Timestamp);
      var order2WithState = TestUtils.getOrder2CreateState(order2Timestamp);
      assertEquals(
          List.of(
              new KeyValue<>(order1WithState.getId(), order1WithState),
              new KeyValue<>(order2WithState.getId(), order2WithState)),
          ret);
    }
  }

  @Test
  public void test_transformDeleteBeforeCreate() {
    long orderTimestamp = Clock.systemUTC().millis();
    var order1 = TestUtils.getOrder1(orderTimestamp);
    try (final TopologyTestDriver topologyTestDriver =
        new TopologyTestDriver(builder.build(), buildStreamsProperties(appConfig))) {
      final TestInputTopic<String, Order> input =
          topologyTestDriver.createInputTopic(
              appConfig.getOrdersTopicName(), stringSerde.serializer(), orderSerde.serializer());
      final TestOutputTopic<String, OrderWithState> output =
          topologyTestDriver.createOutputTopic(
              outTopicName, stringSerde.deserializer(), orderWithStateSerde.deserializer());
      input.pipeInput(order1.getId(), null);
      var ret = output.readKeyValuesToList();
      var order1WithState = TestUtils.order1Deleted(orderTimestamp);
      order1WithState.setOrderTimestamp(ret.get(0).value.getOrderTimestamp());
      assertEquals(List.of(new KeyValue<>(order1WithState.getId(), order1WithState)), ret);
    }
  }

  @Test
  public void test_transformDeleteAfterCreate() {
    long orderTimestamp = Clock.systemUTC().millis();
    var order1 = TestUtils.getOrder1(orderTimestamp);
    var order1Created = TestUtils.getOrder1CreateState(orderTimestamp);

    try (final TopologyTestDriver topologyTestDriver =
        new TopologyTestDriver(builder.build(), buildStreamsProperties(appConfig))) {
      final TestInputTopic<String, Order> input =
          topologyTestDriver.createInputTopic(
              appConfig.getOrdersTopicName(), stringSerde.serializer(), orderSerde.serializer());
      final TestOutputTopic<String, OrderWithState> output =
          topologyTestDriver.createOutputTopic(
              outTopicName, stringSerde.deserializer(), orderWithStateSerde.deserializer());
      input.pipeInput(order1.getId(), order1);
      input.pipeInput(order1.getId(), null);
      var ret = output.readKeyValuesToList();
      var order1Deleted = TestUtils.order1DeletedChildren(ret.get(1).value.getOrderTimestamp());
      assertEquals(
          List.of(
              new KeyValue<>(order1Created.getId(), order1Created),
              new KeyValue<>(order1Deleted.getId(), order1Deleted)),
          ret);
    }
  }
}
