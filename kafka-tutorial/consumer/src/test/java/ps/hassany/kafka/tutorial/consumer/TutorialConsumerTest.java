package ps.hassany.kafka.tutorial.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TutorialConsumerTest {

  @Mock Consumer<String, String> consumer;
  private static final String intputTopc = "testInput";
  private static final Duration pollTimeout = Duration.ofSeconds(1);
  @Mock ConsumerRecords<String, String> consumerRecords;

  @Test
  void test_runConsume() {
    when(consumer.poll(pollTimeout)).thenReturn(consumerRecords);
    TutorialConsumer<String, String> tutorialConsumer =
        new TutorialConsumer<>(consumer, intputTopc, pollTimeout);
    try (var stream = tutorialConsumer.runConsume()) {
      var ret = stream.limit(1).collect(Collectors.toList());
      assertIterableEquals(ret, Arrays.asList(consumerRecords));
    }
    verify(consumer, atLeastOnce()).close();
    verify(consumer, atLeastOnce()).poll(pollTimeout);
  }
}
