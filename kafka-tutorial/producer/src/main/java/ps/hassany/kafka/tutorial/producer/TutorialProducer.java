package ps.hassany.kafka.tutorial.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import ps.hassany.kafka.tutorial.common.Message;

import java.util.Optional;
import java.util.concurrent.Future;

public class TutorialProducer<K, V> {

  private final Producer<K, V> producer;
  private final String outTopic;
  private final Optional<Callback> callback;

  public TutorialProducer(
      final Producer<K, V> producer, final String outTopic, Optional<Callback> callback) {
    this.producer = producer;
    this.outTopic = outTopic;
    this.callback = callback;
  }

  public TutorialProducer(final Producer<K, V> producer, final String outTopic) {
    this(producer, outTopic, Optional.empty());
  }

  public Future<RecordMetadata> produce(final Message<K, V> message) {
    final ProducerRecord<K, V> producerRecord =
        new ProducerRecord<>(outTopic, message.getKey(), message.getValue());
    if (callback.isPresent()) {
      return producer.send(producerRecord, callback.get());
    } else {
      return producer.send(producerRecord);
    }
  }

  public void shutdown() {
    producer.close();
  }
}
