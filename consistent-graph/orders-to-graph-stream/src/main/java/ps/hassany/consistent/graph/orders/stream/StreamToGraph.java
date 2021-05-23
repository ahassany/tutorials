package ps.hassany.consistent.graph.orders.stream;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import ps.hassany.consistent.graph.domain.DomainNode;
import ps.hassany.consistent.graph.domain.DomainRelation;
import ps.hassany.consistent.graph.orders.Order;
import ps.hassany.consistent.graph.orders.stream.mapping.KeyValueMappingSupplier;

import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class StreamToGraph {

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
      KeyValueMappingSupplier<String, Order, String, DomainNode> nodesMapper,
      KeyValueMappingSupplier<String, Order, String, DomainRelation> relationsMapper) {
    StreamsBuilder builder = new StreamsBuilder();
    final Serde<String> stringSerde = new Serdes.StringSerde();
    final SpecificAvroSerde<Order> ordersSerde = ordersSerde(config);
    final SpecificAvroSerde<DomainNode> nodeSerde = nodeSerde(config);
    final SpecificAvroSerde<DomainRelation> relationSerde = relationSerde(config);
    final KStream<String, Order> inputStream =
        builder.stream(config.getOrdersTopicName(), Consumed.with(stringSerde, ordersSerde));
    inputStream
        .flatMap(nodesMapper::map)
        .to(config.getOrdersNodesTopicName(), Produced.with(stringSerde, nodeSerde));

    inputStream
        .flatMap(relationsMapper::map)
        .to(config.getOrdersRelationsTopicName(), Produced.with(stringSerde, relationSerde));

    return builder.build();
  }
}
