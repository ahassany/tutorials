package ps.hassany.kafka.tutorial.common;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SchemaPublication {
  private static final Logger logger = LoggerFactory.getLogger(SchemaPublication.class);

  private final CachedSchemaRegistryClient schemaRegistryClient;

  public SchemaPublication(CachedSchemaRegistryClient schemaRegistryClient) {
    this.schemaRegistryClient = schemaRegistryClient;
  }

  public void registerValueSchema(String topicName, Schema schema)
      throws RestClientException, IOException {
    logger.info(String.format("Registering value schema for topic: %s", topicName));
    schemaRegistryClient.register(String.format("%s-value", topicName), schema);
  }

  public void registerKeySchema(String topicName, Schema schema)
      throws RestClientException, IOException {
    logger.info(String.format("Registering key schema for topic: %s", topicName));
    schemaRegistryClient.register(String.format("%s-key", topicName), schema);
  }
}
