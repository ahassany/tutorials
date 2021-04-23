package ps.hassany.kafka.tutorial.producer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class MessagesProcessorTest {
  @Mock private TutorialProducer<String, String> tutorialProducer;
  @Mock private FileMessagesReader messagesReader;
  @Mock private RecordMetadata recordMetadata1;
  @Mock private RecordMetadata recordMetadata2;

  @Test
  public void test_process() throws IOException {
    List<Message<String, String>> messages =
        Arrays.asList(new Message<>("k1", "v1"), new Message<>("k2", "v2"));
    when(messagesReader.streamMessages()).thenReturn(messages.stream());
    when(tutorialProducer.produce(messages.get(0)))
        .thenReturn(CompletableFuture.completedFuture(recordMetadata1));
    when(tutorialProducer.produce(messages.get(1)))
        .thenReturn(CompletableFuture.completedFuture(recordMetadata2));
    MessagesProcessor<String, String> processor =
        new MessagesProcessor<>(tutorialProducer, messagesReader);

    processor.process();

    verify(tutorialProducer, atLeastOnce()).produce(messages.get(0));
    verify(tutorialProducer, atLeastOnce()).produce(messages.get(1));
  }
}
