package ps.hassany.kafka.tutorial.producer;

import org.apache.kafka.clients.producer.RecordMetadata;
import ps.hassany.kafka.tutorial.common.Message;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MessagesProcessor<K, V> {
  private final TutorialProducer<K, V> producer;
  private final StreamingMessagesReader<K, V> messagesReader;

  public MessagesProcessor(
      TutorialProducer<K, V> producer, StreamingMessagesReader<K, V> messagesReader) {
    this.producer = producer;
    this.messagesReader = messagesReader;
  }

  public void process() throws IOException {
    try (Stream<Message<K, V>> valuesStream = messagesReader.streamMessages()) {
      List<Future<RecordMetadata>> metadata =
          valuesStream.map(producer::produce).collect(Collectors.toList());
      producer.printMetadata(metadata);
    }
  }
}
