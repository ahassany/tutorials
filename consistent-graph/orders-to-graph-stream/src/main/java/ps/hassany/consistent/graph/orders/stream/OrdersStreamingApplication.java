package ps.hassany.consistent.graph.orders.stream;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import ps.hassany.consistent.graph.common.PropertiesClassPathLoader;
import ps.hassany.consistent.graph.orders.stream.mapping.MapOrderToNodes;
import ps.hassany.consistent.graph.orders.stream.mapping.MapOrderToRelations;
import ps.hassany.consistent.graph.orders.stream.mapping.OrderTimestampExtractor;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class OrdersStreamingApplication {
  public static Properties buildStreamsProperties(final OrdersStreamingAppConfig config) {
    Properties streamConfig = new Properties();
    streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getApplicationId());
    streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
    streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    streamConfig.put(SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryURL());
    streamConfig.put(
        StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        OrderTimestampExtractor.class.getName());
    // streamConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    streamConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    // streamConfig.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), 128);
    return streamConfig;
  }

  public static void main(String[] args) throws IOException {
    Properties props = PropertiesClassPathLoader.loadProperties("dev.properties");
    OrdersStreamingAppConfig config = OrdersStreamingAppConfig.build(props);
    StreamToGraph streamToGraph = new StreamToGraph();
    MapOrderToNodes nodesMapper = new MapOrderToNodes();
    MapOrderToRelations relationMapper = new MapOrderToRelations();
    var topology = streamToGraph.buildTopology(config, nodesMapper, relationMapper);
    Properties streamProps = buildStreamsProperties(config);
    final KafkaStreams streams = new KafkaStreams(topology, streamProps);

    final CountDownLatch latch = new CountDownLatch(1);
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread("streams-shutdown-hook") {
              @Override
              public void run() {
                streams.close();
                latch.countDown();
              }
            });

    try {
      streams.start();
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }
    System.exit(0);
  }
}
