package ps.hassany.consistent.graph.orders;

import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ps.hassany.consistent.graph.common.PropertiesClassPathLoader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class OrdersGeneratorTest {
  private OrdersGeneratorAppConfig appConfig;
  MockProducer<String, Order> mockProducer;

  private MockSchemaRegistryClient mockSchemaRegistryClient = new MockSchemaRegistryClient();
  private String registryUrl = "unused";

  public <T> Serde<T> getAvroSerde(boolean isKey) {
    return Serdes.serdeFrom(getSerializer(isKey), getDeserializer(isKey));
  }

  private <T> Serializer<T> getSerializer(boolean isKey) {
    Map<String, Object> map = new HashMap<>();
    map.put(KafkaAvroDeserializerConfig.AUTO_REGISTER_SCHEMAS, true);
    map.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
    Serializer<T> serializer = (Serializer) new KafkaAvroSerializer(mockSchemaRegistryClient);
    serializer.configure(map, isKey);
    return serializer;
  }

  private <T> Deserializer<T> getDeserializer(boolean key) {
    Map<String, Object> map = new HashMap<>();
    map.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
    map.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
    Deserializer<T> deserializer =
        (Deserializer) new KafkaAvroDeserializer(mockSchemaRegistryClient);
    deserializer.configure(map, key);
    return deserializer;
  }

  @BeforeEach
  public void init() throws IOException {
    Properties properties = PropertiesClassPathLoader.loadProperties("test.properties");
    appConfig = OrdersGeneratorAppConfig.build(properties);

    MockSchemaRegistry mockSchemaRegistry;

    var mockSchemaRegistryClient = new MockSchemaRegistryClient();

    var config = Map.of(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
    var specificAvroSerializer = new KafkaAvroSerializer(mockSchemaRegistryClient, config);
    mockProducer =
        new MockProducer<String, Order>(true, new StringSerializer(), getSerializer(false));
  }

  public Order deserializeAvroHttpRequestJSON(byte[] data) throws IOException {
    DatumReader<Order> reader = new SpecificDatumReader<>(Order.class);
    Decoder decoder = DecoderFactory.get().jsonDecoder(Order.getClassSchema(), new String(data));
    return reader.read(null, decoder);
  }

  @Test
  public void test_newOrder() throws IOException {
    OrdersGenerator ordersGenerator = new OrdersGenerator(appConfig, mockProducer, new Random());
    var expectedOrder = ordersGenerator.newOrder(1, 1, 1, 1);
    var serialized = AvroSchemaUtils.toJson(expectedOrder);
    Order order = deserializeAvroHttpRequestJSON(serialized);
    assertNotNull(order);
    assertEquals(order, expectedOrder);
  }

  @Test
  public void test_produce() {
    OrdersGenerator ordersGenerator = new OrdersGenerator(appConfig, mockProducer, new Random());
    ordersGenerator.produce(1);
    assertEquals(mockProducer.history().size(), 1);
  }
}
