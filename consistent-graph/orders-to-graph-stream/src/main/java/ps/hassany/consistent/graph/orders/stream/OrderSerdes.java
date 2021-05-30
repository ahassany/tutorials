package ps.hassany.consistent.graph.orders.stream;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import ps.hassany.consistent.graph.domain.DomainNode;
import ps.hassany.consistent.graph.domain.DomainRelation;
import ps.hassany.consistent.graph.orders.Order;
import ps.hassany.consistent.graph.orders.internal.OrderWithState;
import ps.hassany.consistent.graph.orders.stream.OrdersStreamingAppConfig;

import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class OrderSerdes {
  public static SpecificAvroSerde<Order> ordersSerde(final OrdersStreamingAppConfig appConfig) {
    final SpecificAvroSerde<Order> serde = new SpecificAvroSerde<>();
    Map<String, String> serdeConfig =
        Map.of(SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryURL());
    serde.configure(serdeConfig, false);
    return serde;
  }

  public static SpecificAvroSerde<OrderWithState> orderWithStateSerde(final OrdersStreamingAppConfig appConfig) {
    final SpecificAvroSerde<OrderWithState> serde = new SpecificAvroSerde<>();
    Map<String, String> serdeConfig =
            Map.of(SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryURL());
    serde.configure(serdeConfig, false);
    return serde;
  }


  public static SpecificAvroSerde<DomainNode> nodeSerde(final OrdersStreamingAppConfig appConfig) {
    final SpecificAvroSerde<DomainNode> serde = new SpecificAvroSerde<>();
    Map<String, String> serdeConfig =
        Map.of(SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryURL());
    serde.configure(serdeConfig, false);
    return serde;
  }

  public static SpecificAvroSerde<DomainRelation> relationSerde(
      final OrdersStreamingAppConfig appConfig) {
    final SpecificAvroSerde<DomainRelation> serde = new SpecificAvroSerde<>();
    Map<String, String> serdeConfig =
        Map.of(SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryURL());
    serde.configure(serdeConfig, false);
    return serde;
  }
}
