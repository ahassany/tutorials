package ps.hassany.kafka.tutorial.kstreams.kstreamktable;

import io.confluent.common.utils.TestUtils;
import io.confluent.developer.avro.Publication;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import ps.hassany.kafka.tutorial.common.PropertiesClassPathLoader;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class KStreamToKTableApplication {

  private static SpecificAvroSerde<Publication> publicationSerde(
      final KStreamToKTableConfig appConfig) {
    final SpecificAvroSerde<Publication> serde = new SpecificAvroSerde<>();
    Map<String, String> serdeConfig =
        Map.of(SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryURL());
    serde.configure(serdeConfig, false);
    return serde;
  }

  public static Topology buildTopology(KStreamToKTableConfig config) {
    final StreamsBuilder builder = new StreamsBuilder();
    final Serde<String> stringSerde = Serdes.String();
    final SpecificAvroSerde<Publication> publicationSerde = publicationSerde(config);
    final KStream<String, Publication> stream =
        builder.stream(config.getInputTopicName(), Consumed.with(stringSerde, publicationSerde));
    final KTable<String, Publication> convertedTable =
        stream.toTable(Materialized.as("stream-converted-to-table"));
    stream.to(config.getStreamsOutputTopicName(), Produced.with(stringSerde, publicationSerde));
    convertedTable
        .toStream()
        .to(config.getTableOutputTopicName(), Produced.with(stringSerde, publicationSerde));
    return builder.build();
  }

  public static Properties buildStreamsProperties(final KStreamToKTableConfig config) {
    Properties streamConfig = new Properties();
    streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getApplicationId());
    streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
    streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    streamConfig.put(SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryURL());
    streamConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    streamConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    streamConfig.put(StreamsConfig.producerPrefix(ProducerConfig.BATCH_SIZE_CONFIG), 128);
    return streamConfig;
  }

  public static void main(String[] args) throws Exception {
    Properties props = PropertiesClassPathLoader.loadProperties("dev.properties");
    KStreamToKTableConfig config = KStreamToKTableConfig.build(props);
    System.err.println("Registry URL: " + config.getSchemaRegistryURL());

    var topology = buildTopology(config);
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
