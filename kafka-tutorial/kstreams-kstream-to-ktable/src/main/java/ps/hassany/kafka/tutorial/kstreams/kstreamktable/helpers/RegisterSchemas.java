package ps.hassany.kafka.tutorial.kstreams.kstreamktable.helpers;

import io.confluent.developer.avro.Publication;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.serialization.Serdes;
import ps.hassany.kafka.tutorial.common.PropertiesClassPathLoader;
import ps.hassany.kafka.tutorial.common.SchemaPublication;
import ps.hassany.kafka.tutorial.kstreams.kstreamktable.KStreamToKTableConfig;

import java.util.Properties;

public class RegisterSchemas {

    public static void main(String[] args) throws Exception {
        Properties properties = PropertiesClassPathLoader.loadProperties("dev.properties");
        KStreamToKTableConfig config = KStreamToKTableConfig.build(properties);

        CachedSchemaRegistryClient schemaRegistryClient =
                new CachedSchemaRegistryClient(config.getSchemaRegistryURL(), 10);

        Properties adminClientProps = new Properties();
        adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());

        SchemaPublication schemaPublication = new SchemaPublication(schemaRegistryClient);
        var stringSchema = Schema.create(Schema.Type.STRING);
        schemaPublication.registerKeySchema(config.getStreamsOutputTopicName(), stringSchema);
        schemaPublication.registerValueSchema(config.getInputTopicName(), Publication.SCHEMA$);
        schemaPublication.registerKeySchema(config.getStreamsOutputTopicName(), stringSchema);
        schemaPublication.registerValueSchema(config.getStreamsOutputTopicName(), Publication.SCHEMA$);
        schemaPublication.registerKeySchema(config.getTableOutputTopicName(), stringSchema);
        schemaPublication.registerValueSchema(config.getTableOutputTopicName(), Publication.SCHEMA$);
    }
}
