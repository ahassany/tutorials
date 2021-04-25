package ps.hassany.kafka.tutorial.kstreams.filter;

import io.confluent.developer.avro.Publication;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class FilterEvents {
  private final FilterApplicationConfig appConfig;

  public FilterEvents(FilterApplicationConfig appConfig) {
    this.appConfig = appConfig;
  }

  private SpecificAvroSerde<Publication> publicationSerde() {
    final SpecificAvroSerde<Publication> serde = new SpecificAvroSerde<>();
    Map<String, String> config = new HashMap<>();
    config.put(SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryURL());
    serde.configure(config, false);
    return serde;
  }

  private Topology buildTopology(final SpecificAvroSerde<Publication> publicationSerde) {
    final String inputTopic = appConfig.getInputTopicName();
    final String outputTopic = appConfig.getOutputTopicName();
    final StreamsBuilder builder = new StreamsBuilder();
    builder.stream(inputTopic, Consumed.with(Serdes.String(), publicationSerde))
        .filter((name, publication) -> "George R. R. Martin".equals(publication.getName()))
        .to(outputTopic, Produced.with(Serdes.String(), publicationSerde));
    return builder.build();
  }

  private Properties buildStreamProperties() {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appConfig.getApplicationId());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryURL());
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    return props;
  }

  public void runRecipe() {
    Properties streamProps = buildStreamProperties();
    Topology topology = buildTopology(publicationSerde());
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
