package ps.hassany.consistent.graph.orders;

import gr.james.sampling.ChaoSampling;
import gr.james.sampling.RandomSampling;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ps.hassany.consistent.graph.common.PropertiesClassPathLoader;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

public class DeleteOrderApplication {
  private static final Logger logger = LoggerFactory.getLogger(DeleteOrderApplication.class);

  public static Future<RecordMetadata> deleteOrder(
      final Producer<String, Order> producer, String topicName, final String orderId) {
    logger.info("Deleting order: " + orderId);
    return producer.send(new ProducerRecord<>(topicName, orderId, null));
  }

  public static void main(String[] args) throws Exception {
    // Default 1 orders
    int numOrders = 1;
    if (args.length > 0) {
      numOrders = Integer.parseInt(args[0]);
    }
    logger.info(String.format("Deleting %s orders", numOrders));
    Properties properties = PropertiesClassPathLoader.loadProperties("dev.properties");
    OrdersGeneratorAppConfig appConfig = OrdersGeneratorAppConfig.build(properties);

    Consumer<String, Order> consumer = new KafkaConsumer<>(appConfig.getKakfaConsumerProperties());
    consumer.subscribe(List.of(appConfig.getOrdersTopicName()));
    OrdersConsumer ordersConsumer = new OrdersConsumer(appConfig, consumer);

    Producer<String, Order> producer = new KafkaProducer<>(appConfig.getKakfaProducerProperties());

    RandomSampling<String> rs = new ChaoSampling<>(numOrders, new Random());
    rs.feed(
        ordersConsumer
            .poll()
            .filter(consumerRecord -> consumerRecord.value() != null)
            .map(consumerRecord -> consumerRecord.key())
            .iterator());
    var sample = rs.sample();

    sample.forEach(
        x -> {
          try {
            deleteOrder(producer, appConfig.getOrdersTopicName(), x).get();
          } catch (Exception ex) {
            logger.error("ERROR", ex);
          }
        });
  }
}
