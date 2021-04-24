package ps.hassany.kafka.tutorial.producer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ps.hassany.kafka.tutorial.common.Message;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class TutorialProducerTest {
  private ProducerApplicationConfig producerApplicationConfig;

  @BeforeEach
  public void init() throws IOException {
    producerApplicationConfig = ProducerApplicationConfig.build("app.properties");
  }

  @Test
  public void test_produce() {
    final StringSerializer stringSerializer = new StringSerializer();
    final MockProducer<String, String> mockProducer =
        new MockProducer<>(true, stringSerializer, stringSerializer);
    TutorialProducer<String, String> tutorialProducer =
        new TutorialProducer<>(mockProducer, producerApplicationConfig.getOutTopic());
    List<Message<String, String>> records =
        Arrays.asList(
            new Message<>("k1", "v1"), new Message<>("k2", "v2"), new Message<>("k3", "v3"));

    records.forEach(tutorialProducer::produce);

    List<Message<String, String>> produced =
        mockProducer.history().stream()
            .map(x -> new Message<>(x.key(), x.value()))
            .collect(Collectors.toList());
    assertIterableEquals(records, produced);
  }
}
