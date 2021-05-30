package ps.hassany.consistent.graph.orders.stream;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import ps.hassany.consistent.graph.domain.DomainNode;
import ps.hassany.consistent.graph.domain.DomainRelation;
import ps.hassany.consistent.graph.orders.Order;
import ps.hassany.consistent.graph.orders.internal.DLQRecord;
import ps.hassany.consistent.graph.orders.internal.OrderWithState;

import java.util.Map;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class OrderSerdes {
  
  public static <T extends SpecificRecord> SpecificAvroSerde<T> serde(
      final OrdersStreamingAppConfig appConfig) {
    final SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
    Map<String, String> serdeConfig =
        Map.of(SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryURL());
    serde.configure(serdeConfig, false);
    return serde;
  }
}
