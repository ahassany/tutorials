package ps.hassany.consistent.graph.orders.stream.mapping;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ps.hassany.consistent.graph.common.PropertiesClassPathLoader;
import ps.hassany.consistent.graph.domain.DomainGraphRecord;
import ps.hassany.consistent.graph.orders.internal.OrderWithState;
import ps.hassany.consistent.graph.orders.stream.OrderSerdes;
import ps.hassany.consistent.graph.orders.stream.OrdersStreamingAppConfig;

import java.io.IOException;
import java.time.Clock;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MapOrderToGraphRecordTest {

  private static final String TEST_CONFIG_FILE = "test.properties";
  private final String inTopicName = "input-topic";
  private final String outTopicName = "output-topic";
  private OrdersStreamingAppConfig appConfig;
  private Serde<String> stringSerde;
  private Serde<DomainGraphRecord> graphRecordSerde;
  private Serde<OrderWithState> orderWithStateSerde;
  private StreamsBuilder builder;

  private Properties buildStreamsProperties(final OrdersStreamingAppConfig config) {
    Properties streamConfig = new Properties();
    streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getApplicationId());
    streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
    streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    streamConfig.put(SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryURL());
    streamConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    return streamConfig;
  }

  @BeforeEach
  public void init() throws IOException {
    Properties props = PropertiesClassPathLoader.loadProperties(TEST_CONFIG_FILE);
    appConfig = OrdersStreamingAppConfig.build(props);
    stringSerde = Serdes.String();
    graphRecordSerde = OrderSerdes.serde(appConfig);
    orderWithStateSerde = OrderSerdes.serde(appConfig);
    builder = new StreamsBuilder();
    final KStream<String, OrderWithState> ordersWithStateStream = builder.stream(inTopicName);
    MapOrderToGraphRecord nodesMapper = new MapOrderToGraphRecord();
    ordersWithStateStream
        .flatMap(nodesMapper)
        .to(outTopicName, Produced.with(stringSerde, graphRecordSerde));
  }

  @Test
  public void test_transformOneOrder() {
    long orderTimestamp = Clock.systemUTC().millis();
    var order1WithState = TestUtils.getOrder1CreateState(orderTimestamp);
    try (final TopologyTestDriver topologyTestDriver =
        new TopologyTestDriver(builder.build(), buildStreamsProperties(appConfig))) {
      final TestInputTopic<String, OrderWithState> input =
          topologyTestDriver.createInputTopic(
              inTopicName, stringSerde.serializer(), orderWithStateSerde.serializer());
      final TestOutputTopic<String, DomainGraphRecord> output =
          topologyTestDriver.createOutputTopic(
              outTopicName, stringSerde.deserializer(), graphRecordSerde.deserializer());
      input.pipeInput(order1WithState.getId(), order1WithState);
      var ret = output.readKeyValuesToList();
      assertEquals(7, ret.size());
    }
  }

  @Test
  public void test_transformTwoOrders() {
    long order1Timestamp = Clock.systemUTC().millis() - 1000;
    long order2Timestamp = Clock.systemUTC().millis();
    var order1WithState = TestUtils.getOrder1CreateState(order1Timestamp);
    var order2WithState = TestUtils.getOrder2CreateState(order2Timestamp);
    try (final TopologyTestDriver topologyTestDriver =
        new TopologyTestDriver(builder.build(), buildStreamsProperties(appConfig))) {
      final TestInputTopic<String, OrderWithState> input =
          topologyTestDriver.createInputTopic(
              inTopicName, stringSerde.serializer(), orderWithStateSerde.serializer());
      final TestOutputTopic<String, DomainGraphRecord> output =
          topologyTestDriver.createOutputTopic(
              outTopicName, stringSerde.deserializer(), graphRecordSerde.deserializer());
      input.pipeInput(order1WithState.getId(), order1WithState);
      input.pipeInput(order2WithState.getId(), order2WithState);
      var ret = output.readKeyValuesToList();
      assertEquals(14, ret.size());
    }
  }

  @Test
  public void test_transformDeleteAfterCreate() throws InterruptedException {
    long orderTimestamp = Clock.systemUTC().millis();
    var order1Created = TestUtils.getOrder1CreateState(orderTimestamp);
    var order1Deleted = TestUtils.order1DeletedChildren(orderTimestamp + 105);

    try (final TopologyTestDriver topologyTestDriver =
        new TopologyTestDriver(builder.build(), buildStreamsProperties(appConfig))) {
      final TestInputTopic<String, OrderWithState> input =
          topologyTestDriver.createInputTopic(
              inTopicName, stringSerde.serializer(), orderWithStateSerde.serializer());
      final TestOutputTopic<String, DomainGraphRecord> output =
          topologyTestDriver.createOutputTopic(
              outTopicName, stringSerde.deserializer(), graphRecordSerde.deserializer());
      input.pipeInput(order1Created.getId(), order1Created);
      Thread.sleep(100);
      input.pipeInput(order1Deleted.getId(), order1Deleted);
      var ret = output.readKeyValuesToList();
      assertEquals(14, ret.size());
    }
  }
}
