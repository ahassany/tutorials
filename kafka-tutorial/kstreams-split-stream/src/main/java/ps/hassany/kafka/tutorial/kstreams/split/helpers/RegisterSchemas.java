package ps.hassany.kafka.tutorial.kstreams.split.helpers;

import io.confluent.developer.avro.ActingEvent;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import ps.hassany.kafka.tutorial.common.PropertiesClassPathLoader;
import ps.hassany.kafka.tutorial.common.SchemaPublication;
import ps.hassany.kafka.tutorial.kstreams.split.SplitStreamConfig;

import java.util.Properties;

public class RegisterSchemas {
  public static void main(String[] args) throws Exception {
    Properties properties = PropertiesClassPathLoader.loadProperties("dev.properties");
    SplitStreamConfig config = SplitStreamConfig.build(properties);

    CachedSchemaRegistryClient schemaRegistryClient =
        new CachedSchemaRegistryClient(config.getSchemaRegistryURL(), 10);

    Properties adminClientProps = new Properties();
    adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());

    SchemaPublication schemaPublication = new SchemaPublication(schemaRegistryClient);
    schemaPublication.registerValueSchema(config.getInputTopicName(), ActingEvent.SCHEMA$);
  }
}
