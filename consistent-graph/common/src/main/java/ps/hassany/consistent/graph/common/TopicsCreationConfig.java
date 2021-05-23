package ps.hassany.consistent.graph.common;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;
import java.util.Optional;

@Data
@AllArgsConstructor
public class TopicsCreationConfig {
  private final String topicName;
  private final int topicPartitions;
  private final short topicReplicationFactor;
  private final Optional<Map<String, String>> configs;

  public TopicsCreationConfig(
      final String topicName, final int topicPartitions, final short topicReplicationFactor) {
    this.topicName = topicName;
    this.topicPartitions = topicPartitions;
    this.topicReplicationFactor = topicReplicationFactor;
    this.configs = Optional.empty();
  }
}
