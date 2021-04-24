package ps.hassany.kafka.tutorial.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.Stream;

public class TutorialConsumer<K, V> implements AutoCloseable {
  private volatile boolean keepConsuming = true;
  private final Consumer<K, V> consumer;
  private final String inputTopic;
  private final Duration pollTimeoutDuration;

  public TutorialConsumer(
      Consumer<K, V> consumer, String inputTopic, Duration pollTimeoutDuration) {
    this.consumer = consumer;
    this.inputTopic = inputTopic;
    this.pollTimeoutDuration = pollTimeoutDuration;
  }

  public Stream<ConsumerRecords<K, V>> runConsume() {
    consumer.subscribe(Collections.singleton(inputTopic));
    return Stream.generate(
            () -> {
              final ConsumerRecords<K, V> consumerRecords = consumer.poll(pollTimeoutDuration);
              return consumerRecords;
            })
        .onClose(this::close);
  }

  @Override
  public void close() {
    consumer.close();
  }
}
