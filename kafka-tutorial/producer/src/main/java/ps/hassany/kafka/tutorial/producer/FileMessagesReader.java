package ps.hassany.kafka.tutorial.producer;

import io.vavr.CheckedFunction1;
import ps.hassany.kafka.tutorial.common.Message;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class FileMessagesReader<K, V> implements StreamingMessagesReader<K, V> {
  private final Path inputFile;
  private final MessageLineParser<K, V> messageLineParser;

  public FileMessagesReader(Path inputFile, MessageLineParser<K, V> messageLineParser) {
    this.inputFile = inputFile;
    this.messageLineParser = messageLineParser;
  }

  @Override
  public Stream<Message<K, V>> streamMessages() throws IOException {
    Stream<String> linesStream = Files.lines(inputFile);
    CheckedFunction1<String, Message<K, V>> f = x -> messageLineParser.parseMessage(x);
    return linesStream.filter(l -> !l.trim().isEmpty()).map(f.unchecked());
  }
}
