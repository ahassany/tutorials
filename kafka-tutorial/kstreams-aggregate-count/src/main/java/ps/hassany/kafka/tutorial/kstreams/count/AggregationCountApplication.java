package ps.hassany.kafka.tutorial.kstreams.count;

import io.confluent.common.utils.TestUtils;
import io.confluent.developer.avro.TicketSale;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Produced;
import ps.hassany.kafka.tutorial.common.PropertiesClassPathLoader;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class AggregationCountApplication {
  private SpecificAvroSerde<TicketSale> ticketSaleSerde(final Properties props) {
    final SpecificAvroSerde<TicketSale> serde = new SpecificAvroSerde<>();
    Map<String, String> config = (Map) props;
    serde.configure(config, false);
    return serde;
  }

  public Topology buildTopology(
      Properties props, final SpecificAvroSerde<TicketSale> ticketSaleSerde) {
    final StreamsBuilder builder = new StreamsBuilder();
    final String inputTopic = props.getProperty("input.topic.name");
    final String outputTopic = props.getProperty("output.topic.name");

    builder.stream(inputTopic, Consumed.with(Serdes.String(), ticketSaleSerde))
        .map((k, v) -> new KeyValue<>(v.getTitle(), v.getTicketTotalValue()))
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
        .count()
        .toStream()
        .mapValues(v -> v.toString() + " tickets sold")
        .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
    return builder.build();
  }

  public void createTopics(Properties properties) {
    AdminClient client = AdminClient.create(properties);
    List<NewTopic> topics =
        List.of(
            new NewTopic(
                properties.getProperty("input.topic.name"),
                Integer.parseInt(properties.getProperty("input.topic.partitions")),
                Short.parseShort(properties.getProperty("input.topic.replication.factor"))),
            new NewTopic(
                properties.getProperty("output.topic.name"),
                Integer.parseInt(properties.getProperty("output.topic.partitions")),
                Short.parseShort(properties.getProperty("output.topic.replication.factor"))));
    client.createTopics(topics);
    client.close();
  }

  public static void main(String[] args) throws IOException {
    new AggregationCountApplication().runRecipe();
  }

  private void runRecipe() throws IOException {
    final Properties allProps = PropertiesClassPathLoader.loadProperties("dev.properties");
    allProps.put(StreamsConfig.APPLICATION_ID_CONFIG, allProps.getProperty("application.id"));
    allProps.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
    allProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    Topology topology = this.buildTopology(allProps, this.ticketSaleSerde(allProps));
    this.createTopics(allProps);

    final KafkaStreams streams = new KafkaStreams(topology, allProps);
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
      streams.cleanUp();
      streams.start();
      latch.await();
    } catch (Throwable e) {
      System.exit(1);
    }
    System.exit(0);
  }
}
