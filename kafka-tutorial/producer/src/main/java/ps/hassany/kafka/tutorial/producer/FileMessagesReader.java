package ps.hassany.kafka.tutorial.producer;

import ps.hassany.kafka.tutorial.common.Message;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class FileMessagesReader implements StreamingMessagesReader<String, String> {
  private final Path inputFile;
  private final StringMessageParser stringMessageParser;

  public FileMessagesReader(Path inputFile, StringMessageParser stringMessageParser) {
    this.inputFile = inputFile;
    this.stringMessageParser = stringMessageParser;
  }

  @Override
  public Stream<Message<String, String>> streamMessages() throws IOException {
    Stream<String> linesStream = Files.lines(inputFile);
    return linesStream.filter(l -> !l.trim().isEmpty()).map(stringMessageParser::parseMessage);
  }
}
