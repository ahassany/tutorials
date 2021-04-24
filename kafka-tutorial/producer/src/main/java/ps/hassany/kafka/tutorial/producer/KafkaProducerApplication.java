package ps.hassany.kafka.tutorial.producer;

import org.apache.kafka.clients.producer.KafkaProducer;

import java.io.IOException;
import java.util.Optional;

public class KafkaProducerApplication {

  public static void runApp(ProducerApplicationConfig appConfig) {
    final KafkaProducer<String, String> kafkaProducer =
        new KafkaProducer<>(appConfig.getKakfaProducerProperties());
    final TutorialProducer<String, String> tutorialProducer =
        new TutorialProducer<>(
            kafkaProducer,
            appConfig.getOutTopic(),
            Optional.of(
                (recordMetadata, exception) -> {
                  if (exception != null) {
                    System.out.println("Exception: " + exception.getMessage());
                    exception.printStackTrace(System.err);
                  } else {
                    System.out.printf(
                        "Callback: Partition: %d, Offset: %d, Exception: None%s",
                        recordMetadata.partition(),
                        recordMetadata.offset(),
                        System.lineSeparator());
                  }
                }));
    final StringMessageParser stringMessageParser =
        new StringMessageParser(appConfig.getMessageDelimiter(), appConfig.getDefaultKey());
    StreamingMessagesReader<String, String> messagesReader =
        new FileMessagesReader<>(appConfig.getMessagesFilePath(), stringMessageParser);
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
    final ProducerApplicationConfig producerApplicationConfig;
    try {
      producerApplicationConfig = ProducerApplicationConfig.build("app.properties");
    } catch (IOException exception) {
      System.err.printf("Couldn't load properties: %s", exception.getMessage());
      return;
    }
    runApp(producerApplicationConfig);
  }
}
