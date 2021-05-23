package ps.hassany.consistent.graph.orders;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ps.hassany.consistent.graph.common.PropertiesClassPathLoader;

import java.util.Properties;

public class OrdersGeneratorApplication {
  private static final Logger logger = LoggerFactory.getLogger(OrdersGeneratorApplication.class);

  public static void main(String[] args) throws Exception {
    Properties properties = PropertiesClassPathLoader.loadProperties("dev.properties");
    OrdersGeneratorAppConfig appConfig = OrdersGeneratorAppConfig.build(properties);

    Properties adminClientProps = new Properties();
    adminClientProps.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
    AdminClient adminClient = AdminClient.create(adminClientProps);

    CachedSchemaRegistryClient schemaRegistryClient =
        new CachedSchemaRegistryClient(appConfig.getSchemaRegistryURL(), 10);

    var ordersGenerator = new OrdersGenerator(appConfig, adminClient, schemaRegistryClient);

    ordersGenerator.createTopics();
    ordersGenerator.registerSchemas();
    ordersGenerator.produce(10);
  }
}
