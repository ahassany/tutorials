package ps.hassany.kafka.tutorial.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


import ps.hassany.kafka.tutorial.common.Message;

public class TutorialProducer<K, V> {

  private final Producer<K, V> producer;
  private final String outTopic;

  public TutorialProducer(final Producer<K, V> producer, final String outTopic) {
    this.producer = producer;
    this.outTopic = outTopic;
  }

  public Future<RecordMetadata> produce(final Message<K, V> message) {
    final var producerRecord = new ProducerRecord<>(outTopic, message.getKey(), message.getValue());
    return producer.send(producerRecord);
  }

  public void shutdown() {
    producer.close();
  }

  public void printMetadata(final Collection<Future<RecordMetadata>> metadata) {
    System.out.println("Offsets and timestamps committed in batch to topic " + outTopic);
    metadata.forEach(
        m -> {
          try {
            final var recordMetadata = m.get();
            System.out.printf(
                "Record written to offset %d timestamp %d%s",
                recordMetadata.offset(), recordMetadata.timestamp(), System.lineSeparator());
          } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
          }
        });
  }
}
