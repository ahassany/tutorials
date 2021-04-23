package ps.hassany.kafka.tutorial.producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class FileMessagesReaderTest {
  private Config config;

  @BeforeEach
  public void init() throws IOException {
    config = Config.build("app.properties");
  }

  @Test
  public void test_streamMessages() throws IOException {
    FileMessagesReader fileMessagesReader =
        new FileMessagesReader(
            config.getMessagesFilePath(),
            new StringMessageParser(config.getMessageDelimiter(), config.getDefaultKey()));
    List<Message<String, String>> expected =
        Arrays.asList(
            new Message<>("1", "value"),
            new Message<>("2", "words"),
            new Message<>(config.getDefaultKey(), "withoutkey"));
    try (Stream<Message<String, String>> messagesStream = fileMessagesReader.streamMessages()) {
      var actual = messagesStream.collect(Collectors.toList());
      assertIterableEquals(expected, actual);
    }
  }
}
