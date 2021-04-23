package ps.hassany.kafka.tutorial.producer;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;

public class KafkaProducerApplication {

  public static void runApp(Config config) {
    final KafkaProducer<String, String> kafkaProducer =
        new KafkaProducer<>(config.getKakfaProducerProperties());
    final TutorialProducer<String, String> tutorialProducer =
        new TutorialProducer<>(kafkaProducer, config.getOutTopic());
    final StringMessageParser stringMessageParser =
        new StringMessageParser(config.getMessageDelimiter(), config.getDefaultKey());
    StreamingMessagesReader messagesReader =
        new FileMessagesReader(config.getMessagesFilePath(), stringMessageParser);
    final MessagesProcessor<String, String> messagesProcessor =
        new MessagesProcessor<>(tutorialProducer, messagesReader);
    try {
      messagesProcessor.process();
    } catch (Exception exception) {
      System.err.printf("Couldn't read messages file: %s", exception.getMessage());
    } finally {
      tutorialProducer.shutdown();
    }
  }

  public static void main(String[] args) {
    final Config config;
    try {
      config = Config.build("app.properties");
    } catch (IOException exception) {
      System.err.printf("Couldn't load properties: %s", exception.getMessage());
      return;
    }
    runApp(config);
  }
}
