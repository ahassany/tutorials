package ps.hassany.kafka.tutorial.common;

import io.vavr.control.Try;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class TopicCreation {
  private static final Logger logger = LoggerFactory.getLogger(TopicCreation.class);
  private final AdminClient adminClient;

  public TopicCreation(final AdminClient adminClient) {
    this.adminClient = adminClient;
  }

  protected NewTopic createTopic(
      final String topicName, final int partitions, final short replicationFactor) {
    return new NewTopic(topicName, partitions, replicationFactor);
  }

  private Map<String, NewTopic> onlyNewTopics(final Collection<TopicsCreationConfig> configs)
      throws InterruptedException, ExecutionException {
    var existingTopicsResults = adminClient.listTopics();
    var existingTopicsNames = existingTopicsResults.names().get();
    return configs.stream()
        .filter(
            (config) -> {
              if (existingTopicsNames.contains(config.getTopicName())) {
                logger.info(
                    String.format(
                        "Skip creating topic %s since it already exists", config.getTopicName()));
                return false;
              }
              return true;
            })
        .collect(
            Collectors.toMap(
                TopicsCreationConfig::getTopicName,
                config -> {
                  var newTopic =
                      new NewTopic(
                          config.getTopicName(),
                          config.getTopicPartitions(),
                          config.getTopicReplicationFactor());
                  return config.getConfigs().map(x -> newTopic.configs(x)).orElse(newTopic);
                },
                (a, b) -> b));
  }

  public void createTopics(final Collection<TopicsCreationConfig> configs)
      throws InterruptedException, ExecutionException {
    var topics = onlyNewTopics(configs);
    var result = adminClient.createTopics(topics.values());
    result
        .values()
        .forEach(
            (topicName, future) -> {
              var topic = topics.get(topicName);
              future.whenComplete(
                  (aVoid, maybeError) ->
                      Optional.ofNullable(maybeError)
                          .map(Try::<Void>failure)
                          .orElse(Try.success(null))
                          .onFailure(
                              throwable ->
                                  logger.error("Topic creation didn't complete:", throwable))
                          .onSuccess(
                              (anOtherVoid) ->
                                  logger.info(
                                      String.format(
                                          "Topic %s, has been successfully created "
                                              + "with %s partitions and replicated %s times",
                                          topic.name(),
                                          topic.numPartitions(),
                                          topic.replicationFactor()))));
            });
    result.all().get();
  }
}
