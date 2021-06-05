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
import ps.hassany.consistent.graph.domain.DomainGraphRecord;
import ps.hassany.consistent.graph.orders.Order;
import ps.hassany.consistent.graph.orders.internal.DLQRecord;
import ps.hassany.consistent.graph.orders.internal.OrderWithState;
import ps.hassany.consistent.graph.orders.stream.mapping.OrderDiffTransformerSupplier;

import java.time.Duration;

public class StreamToGraph {

  private static final String ordersStoreName = "order-state-store";

  public Topology buildTopology(
      OrdersStreamingAppConfig config,
      KeyValueMapper<String, OrderWithState, Iterable<KeyValue<String, DomainGraphRecord>>>
          recordsMapper) {
    StreamsBuilder builder = new StreamsBuilder();
    final Serde<String> stringSerde = new Serdes.StringSerde();
    final SpecificAvroSerde<Order> ordersSerde = OrderSerdes.serde(config);
    final SpecificAvroSerde<DomainGraphRecord> recordSerde = OrderSerdes.serde(config);
    final SpecificAvroSerde<DLQRecord> dlqSerde = OrderSerdes.serde(config);

    final Duration windowSize = Duration.ofMinutes(100);
    final Duration retentionPeriod = windowSize;

    final StoreBuilder<WindowStore<String, Order>> ordersStoreBuilder =
        Stores.windowStoreBuilder(
            Stores.persistentWindowStore(ordersStoreName, retentionPeriod, windowSize, false),
            stringSerde,
            ordersSerde);

    builder.addStateStore(ordersStoreBuilder);

    final KStream<String, Order> inputStream =
        builder.stream(config.getOrdersTopicName(), Consumed.with(stringSerde, ordersSerde));

    final KStream<String, DLQRecord>[] orderStateStreams =
        inputStream
            .transform(
                new OrderDiffTransformerSupplier(
                    ordersStoreName, windowSize.toMillis(), ordersStoreBuilder))
            .branch((key, value) -> value.getIsError(), (key, value) -> !value.getIsError());

    // DLQ output
    orderStateStreams[0].to(config.getOrdersDLQTopicName(), Produced.with(stringSerde, dlqSerde));

    final KStream<String, OrderWithState> orderStateStream =
        orderStateStreams[1].map((key, value) -> new KeyValue<>(key, value.getOrderWithState()));

    orderStateStream
        .flatMap(recordsMapper)
        .to(config.getOrdersNodesTopicName(), Produced.with(stringSerde, recordSerde));

    return builder.build();
  }
}
