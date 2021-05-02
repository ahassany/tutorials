package ps.hassany.kafka.tutorial.kstreams.distinct.helpers;

import io.confluent.developer.avro.Click;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import ps.hassany.kafka.tutorial.common.PropertiesClassPathLoader;
import ps.hassany.kafka.tutorial.common.SchemaPublication;
import ps.hassany.kafka.tutorial.kstreams.distinct.FindDisticntEventsApplicationConfig;

import java.util.Properties;

public class RegisterSchemas {
  public static void main(String[] args) throws Exception {
    Properties properties = PropertiesClassPathLoader.loadProperties("dev.properties");
    FindDisticntEventsApplicationConfig config =
        FindDisticntEventsApplicationConfig.build(properties);

    CachedSchemaRegistryClient schemaRegistryClient =
        new CachedSchemaRegistryClient(config.getSchemaRegistryURL(), 10);

    Properties adminClientProps = new Properties();
    adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());

    SchemaPublication schemaPublication = new SchemaPublication(schemaRegistryClient);
    schemaPublication.registerValueSchema(config.getInputTopicName(), Click.SCHEMA$);
  }
}
