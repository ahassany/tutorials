package ps.hassany.kafka.tutorial.kstreams.split;

import io.confluent.developer.avro.ActingEvent;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import ps.hassany.kafka.tutorial.common.PropertiesClassPathLoader;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class SplitStreamApplication {
  public Properties buildStreamsProperties(final SplitStreamConfig config) {
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getApplicationId());
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    props.put(SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryURL());
    return props;
  }

  public Topology buildTopology(final SplitStreamConfig config) {
    final StreamsBuilder builder = new StreamsBuilder();
    KStream<String, ActingEvent>[] branches =
        builder.<String, ActingEvent>stream(config.getInputTopicName())
            .branch(
                (key, appearance) -> "drama".equals(appearance.getGenre()),
                (key, appearance) -> "fantasy".equals(appearance.getGenre()),
                (key, appearance) -> true);
    branches[0].to(config.getDramaTopicName());
    branches[1].to(config.getFantasyTopicName());
    branches[2].to(config.getOtherTopicName());

    return builder.build();
  }

  public static void main(String[] args) throws Exception {
    Properties props = PropertiesClassPathLoader.loadProperties("dev.properties");
    SplitStreamConfig config = SplitStreamConfig.build(props);
    SplitStreamApplication ss = new SplitStreamApplication();
    Properties streamProps = ss.buildStreamsProperties(config);
    Topology topology = ss.buildTopology(config);
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
