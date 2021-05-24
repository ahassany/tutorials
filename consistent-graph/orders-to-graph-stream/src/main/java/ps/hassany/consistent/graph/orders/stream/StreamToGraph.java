package ps.hassany.consistent.graph.orders.stream;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import ps.hassany.consistent.graph.domain.DomainNode;
import ps.hassany.consistent.graph.domain.DomainRelation;
import ps.hassany.consistent.graph.orders.Order;
import ps.hassany.consistent.graph.orders.internal.OrderWithState;
import ps.hassany.consistent.graph.orders.stream.mapping.OrderDiffTransformerSupplier;

import java.time.Duration;
import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class StreamToGraph {

  private static final String ordersStoreName = "order-state-store";

  private SpecificAvroSerde<Order> ordersSerde(final OrdersStreamingAppConfig appConfig) {
    final SpecificAvroSerde<Order> serde = new SpecificAvroSerde<>();
    Map<String, String> serdeConfig =
        Map.of(SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryURL());
    serde.configure(serdeConfig, false);
    return serde;
  }

  private SpecificAvroSerde<DomainNode> nodeSerde(final OrdersStreamingAppConfig appConfig) {
    final SpecificAvroSerde<DomainNode> serde = new SpecificAvroSerde<>();
    Map<String, String> serdeConfig =
        Map.of(SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryURL());
    serde.configure(serdeConfig, false);
    return serde;
  }

  private SpecificAvroSerde<DomainRelation> relationSerde(
      final OrdersStreamingAppConfig appConfig) {
    final SpecificAvroSerde<DomainRelation> serde = new SpecificAvroSerde<>();
    Map<String, String> serdeConfig =
        Map.of(SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryURL());
    serde.configure(serdeConfig, false);
    return serde;
  }

  public Topology buildTopology(
      OrdersStreamingAppConfig config,
      KeyValueMapper<String, OrderWithState, Iterable<KeyValue<String, DomainNode>>> nodesMapper,
      KeyValueMapper<String, OrderWithState, Iterable<KeyValue<String, DomainRelation>>>
          relationsMapper) {
    StreamsBuilder builder = new StreamsBuilder();
    final Serde<String> stringSerde = new Serdes.StringSerde();
    final SpecificAvroSerde<Order> ordersSerde = ordersSerde(config);
    final SpecificAvroSerde<DomainNode> nodeSerde = nodeSerde(config);
    final SpecificAvroSerde<DomainRelation> relationSerde = relationSerde(config);

    final Duration windowSize = Duration.ofMinutes(1);
    final Duration retentionPeriod = windowSize;

    final StoreBuilder<WindowStore<String, Order>> ordersStoreBuilder =
        Stores.windowStoreBuilder(
            Stores.persistentWindowStore(ordersStoreName, retentionPeriod, windowSize, false),
            stringSerde,
            ordersSerde);

    builder.addStateStore(ordersStoreBuilder);

    final KStream<String, Order> inputStream =
        builder.stream(config.getOrdersTopicName(), Consumed.with(stringSerde, ordersSerde));

    final KStream<String, OrderWithState> orderStateStream =
        inputStream.transform(
            new OrderDiffTransformerSupplier(
                ordersStoreName, windowSize.toMillis(), ordersStoreBuilder));

    orderStateStream
        .flatMap(nodesMapper)
        .to(config.getOrdersNodesTopicName(), Produced.with(stringSerde, nodeSerde));

    orderStateStream
        .flatMap(relationsMapper)
        .to(config.getOrdersRelationsTopicName(), Produced.with(stringSerde, relationSerde));

    return builder.build();
  }
}
