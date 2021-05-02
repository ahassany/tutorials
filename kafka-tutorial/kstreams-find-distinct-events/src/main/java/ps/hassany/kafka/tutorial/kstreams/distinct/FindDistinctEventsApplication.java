package ps.hassany.kafka.tutorial.kstreams.distinct;

import io.confluent.common.utils.TestUtils;
import io.confluent.developer.avro.Click;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import ps.hassany.kafka.tutorial.common.PropertiesClassPathLoader;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class FindDistinctEventsApplication {
  private static final String storeName = "eventId-store";

  private static class DeduplicationTransformer<K, V, E>
      implements ValueTransformerWithKey<K, V, V> {
    private ProcessorContext context;
    private WindowStore<E, Long> eventIdStore;
    private final long leftDurationMs;
    private final long rightDurationMs;
    private final KeyValueMapper<K, V, E> idExtractor;

    private DeduplicationTransformer(
        final long maintainDurationPerEventInMs, final KeyValueMapper<K, V, E> idExtractor) {
      if (maintainDurationPerEventInMs < 1) {
        throw new IllegalArgumentException("maintain duration per event must be >= 1");
      }
      this.leftDurationMs = maintainDurationPerEventInMs / 2;
      this.rightDurationMs = maintainDurationPerEventInMs - leftDurationMs;
      this.idExtractor = idExtractor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
      this.context = context;
      eventIdStore = (WindowStore<E, Long>) context.getStateStore(storeName);
    }

    @Override
    public V transform(final K key, final V value) {
      final E eventId = idExtractor.apply(key, value);
      if (eventId == null) {
        return value;
      }
      final V output;
      if (isDuplicate(eventId)) {
        output = null;
        updateTimestampOfExistingEventToPreventExpiry(eventId, context.timestamp());
      } else {
        output = value;
        rememberNewEvent(eventId, context.timestamp());
      }
      return output;
    }

    private boolean isDuplicate(final E eventId) {
      final long eventTime = context.timestamp();
      final WindowStoreIterator<Long> timeIterator =
          eventIdStore.fetch(eventId, eventTime - leftDurationMs, eventTime + rightDurationMs);
      final boolean isDuplicate = timeIterator.hasNext();
      timeIterator.close();
      return isDuplicate;
    }

    private void updateTimestampOfExistingEventToPreventExpiry(
        final E eventId, final long newTimestamp) {
      eventIdStore.put(eventId, newTimestamp, newTimestamp);
    }

    private void rememberNewEvent(final E eventId, final long timestamp) {
      eventIdStore.put(eventId, timestamp, timestamp);
    }

    @Override
    public void close() {}
  }

  private SpecificAvroSerde<Click> buildClickSerde(
      final FindDisticntEventsApplicationConfig config) {
    final SpecificAvroSerde<Click> serde = new SpecificAvroSerde<>();
    final Map<String, String> opts =
        Map.of(SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryURL());
    serde.configure(opts, false);
    return serde;
  }

  public Topology buildTopology(
      final FindDisticntEventsApplicationConfig config, final SpecificAvroSerde<Click> clickSerde) {
    final StreamsBuilder builder = new StreamsBuilder();

    // How long we "remember" an event.  During this time, any incoming duplicates of the event
    // will be, well, dropped, thereby de-duplicating the input data.
    final Duration windowSize = Duration.ofMinutes(2);
    // retention period must be at least window size -- for this use case, we don't need a longer
    // retention period
    // and thus just use the window size as retention time
    final Duration retentionPeriod = windowSize;

    final StoreBuilder<WindowStore<String, Long>> deupStoreBuilder =
        Stores.windowStoreBuilder(
            Stores.persistentWindowStore(storeName, retentionPeriod, windowSize, false),
            Serdes.String(),
            Serdes.Long());

    builder.addStateStore(deupStoreBuilder);
    builder.stream(config.getInputTopicName(), Consumed.with(Serdes.String(), clickSerde))
        .transformValues(
            () ->
                new DeduplicationTransformer<>(
                    windowSize.toMillis(), (key, value) -> value.getIp()),
            storeName)
        .filter((k, v) -> v != null)
        .to(config.getOutputTopicName(), Produced.with(Serdes.String(), clickSerde));
    return builder.build();
  }

  public static void main(String[] args) throws IOException {
    new FindDistinctEventsApplication().runRecipe();
  }

  private void runRecipe() throws IOException {
    var props = PropertiesClassPathLoader.loadProperties("dev.properties");
    var config = FindDisticntEventsApplicationConfig.build(props);
    var streamConfig = new Properties();

    streamConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getApplicationId());
    streamConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
    streamConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    streamConfig.put(SCHEMA_REGISTRY_URL_CONFIG, config.getSchemaRegistryURL());
    streamConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());

    final Topology topology = buildTopology(config, buildClickSerde(config));
    final KafkaStreams streams = new KafkaStreams(topology, streamConfig);
    final CountDownLatch latch = new CountDownLatch(1);

    // Attach shutdown handler to catch Control-C.
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread("streams-shutdown-hook") {
              @Override
              public void run() {
                streams.close(Duration.ofSeconds(5));
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
