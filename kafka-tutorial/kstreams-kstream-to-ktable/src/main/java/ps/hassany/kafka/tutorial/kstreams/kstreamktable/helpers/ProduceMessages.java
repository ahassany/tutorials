package ps.hassany.kafka.tutorial.kstreams.kstreamktable.helpers;

import io.confluent.developer.avro.Publication;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import ps.hassany.kafka.tutorial.common.PropertiesClassPathLoader;
import ps.hassany.kafka.tutorial.kstreams.kstreamktable.KStreamToKTableConfig;
import ps.hassany.kafka.tutorial.producer.*;

import java.util.Optional;
import java.util.Properties;

public class ProduceMessages {
  private static Callback printCallback =
      (recordMetadata, exception) -> {
        if (exception != null) {
          System.out.println("Exception: " + exception.getMessage());
          exception.printStackTrace(System.err);
        } else {
          System.out.printf(
              "Callback: Partition: %d, Offset: %d, Exception: None%s",
              recordMetadata.partition(), recordMetadata.offset(), System.lineSeparator());
        }
      };

  public static void produce(KStreamToKTableConfig appConfig) {
    final KafkaProducer<String, Publication> kafkaProducer =
        new KafkaProducer<>(appConfig.getKakfaProducerProperties());
    final TutorialProducer<String, Publication> tutorialProducer =
        new TutorialProducer<>(
            kafkaProducer, appConfig.getTableOutputTopicName(), Optional.of(printCallback));

    final JsonToAvroWithKeyMessageParser<String, Publication> messageParser =
        new JsonToAvroWithKeyMessageParser<>(Publication.getClassSchema(), "name");
    StreamingMessagesReader<String, Publication> messagesReader =
        new FileMessagesReader<>(appConfig.getMessagesFilePath(), messageParser);
    final MessagesProcessor<String, Publication> messagesProcessor =
        new MessagesProcessor<>(tutorialProducer, messagesReader);
    try {
      messagesProcessor.process();
    } catch (Exception exception) {
      System.err.printf("Couldn't read messages file: %s", exception.getMessage());
      exception.printStackTrace();
    } finally {
      tutorialProducer.shutdown();
    }
  }

  public static void main(String[] args) throws Exception {
    Properties properties = PropertiesClassPathLoader.loadProperties("dev.properties");
    KStreamToKTableConfig config = KStreamToKTableConfig.build(properties);
    for (int i = 0; i < 10000; i++) {
      produce(config);
    }
  }
}
