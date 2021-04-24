package ps.hassany.kafka.tutorial.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import ps.hassany.kafka.tutorial.common.Message;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class FileMessagesReaderTest {
  private ProducerApplicationConfig applicationConfig;

  @BeforeEach
  public void init() throws IOException {
    applicationConfig = ProducerApplicationConfig.build("app.properties");
  }

  @Test
  public void test_streamMessages() throws IOException {
    FileMessagesReader fileMessagesReader =
        new FileMessagesReader(
            applicationConfig.getMessagesFilePath(),
            new StringMessageParser(applicationConfig.getMessageDelimiter(), applicationConfig.getDefaultKey()));
    List<Message<String, String>> expected =
        Arrays.asList(
            new Message<>("1", "value"),
            new Message<>("2", "words"),
            new Message<>(applicationConfig.getDefaultKey(), "withoutkey"));
    try (Stream<Message<String, String>> messagesStream = fileMessagesReader.streamMessages()) {
      var actual = messagesStream.collect(Collectors.toList());
      assertIterableEquals(expected, actual);
    }
  }
}
