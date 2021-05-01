package ps.hassany.kafka.tutorial.common;

import lombok.Data;

@Data
public class TopicsCreationConfig {
  private final String topicName;
  private final int topicPartitions;
  private final short topicReplicationFactor;
}
