package ps.hassany.kafka.tutorial.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import ps.hassany.kafka.tutorial.common.Message;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ConsumerApplication {

  public static void runApp(ConsumerApplicationConfig appConfig) {
    ConsumerRecordsHandler<String, String, List<Message<String, String>>> consumerRecordsHandler =
        (x) ->
            StreamSupport.stream(x.spliterator(), false)
                .map((r) -> new Message<>(r.key(), r.value()))
                .collect(Collectors.toList());
    Consumer<String, String> kafkaConsumer =
        new KafkaConsumer<>(appConfig.getKafkaConsumerProperties());
    TutorialConsumer<String, String> tutorialConsumer =
        new TutorialConsumer<>(
            kafkaConsumer, appConfig.getInputTopic(), appConfig.getPollTimeoutDuration());
    Runtime.getRuntime().addShutdownHook(new Thread(tutorialConsumer::close));
    try (var topicStream = tutorialConsumer.runConsume()) {
      topicStream
          .map(consumerRecordsHandler::process)
          .forEach((x) -> x.forEach(y -> System.out.println(y.getKey() + " : " + y.getValue())));
    }
  }

  public static void main(String[] args) {
    final ConsumerApplicationConfig appConfig;
    try {
      appConfig = ConsumerApplicationConfig.build("app.properties");
    } catch (IOException exception) {
      System.err.printf("Couldn't load properties: %s", exception.getMessage());
      return;
    }
    runApp(appConfig);
  }
}
