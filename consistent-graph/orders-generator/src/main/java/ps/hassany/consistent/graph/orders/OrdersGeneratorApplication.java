package ps.hassany.consistent.graph.orders;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ps.hassany.consistent.graph.common.PropertiesClassPathLoader;

import java.util.Properties;
import java.util.Random;

public class OrdersGeneratorApplication {
  private static final Logger logger = LoggerFactory.getLogger(OrdersGeneratorApplication.class);

  public static void main(String[] args) throws Exception {
    // Default 10 orders
    int numOrders = 10;
    if (args.length > 0) {
      numOrders = Integer.parseInt(args[0]);
    }
    logger.info(String.format("Producing %s new orders", numOrders));

    Properties properties = PropertiesClassPathLoader.loadProperties("dev.properties");
    OrdersGeneratorAppConfig appConfig = OrdersGeneratorAppConfig.build(properties);

    Properties adminClientProps = new Properties();
    adminClientProps.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
    AdminClient adminClient = AdminClient.create(adminClientProps);

    CachedSchemaRegistryClient schemaRegistryClient =
        new CachedSchemaRegistryClient(appConfig.getSchemaRegistryURL(), 10);

    Helpers helpers = new Helpers(appConfig, adminClient, schemaRegistryClient);
    helpers.createTopics();
    helpers.registerSchemas();
    var kafkaProducer = new KafkaProducer<String, Order>(appConfig.getKakfaProducerProperties());
    var ordersGenerator = new OrdersGenerator(appConfig, kafkaProducer, new Random());
    ordersGenerator.produce(numOrders);
  }
}
