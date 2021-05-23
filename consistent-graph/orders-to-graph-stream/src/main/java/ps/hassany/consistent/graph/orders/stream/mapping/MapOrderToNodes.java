package ps.hassany.consistent.graph.orders.stream.mapping;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import ps.hassany.consistent.graph.domain.DomainNode;
import ps.hassany.consistent.graph.domain.NodePayloadType;
import ps.hassany.consistent.graph.orders.BookOrder;
import ps.hassany.consistent.graph.orders.LaptopOrder;
import ps.hassany.consistent.graph.orders.Order;
import ps.hassany.consistent.graph.orders.OrderedItem;
import ps.hassany.consistent.graph.orders.payload.BookNodePayload;
import ps.hassany.consistent.graph.orders.payload.CustomerNodePayload;
import ps.hassany.consistent.graph.orders.payload.LaptopNodePayload;
import ps.hassany.consistent.graph.orders.payload.OrderNodePayload;

import java.util.LinkedList;
import java.util.List;

public class MapOrderToNodes
    implements KeyValueMapper<String, Order, Iterable<KeyValue<String, DomainNode>>> {

  @Override
  public Iterable<KeyValue<String, DomainNode>> apply(String key, Order order) {
    List<KeyValue<String, DomainNode>> nodes = new LinkedList<>();
    String orderId = OrderMappingUtils.getOrderId(order);
    DomainNode orderNode =
        DomainNode.newBuilder()
            .setId(orderId)
            .setProducer(OrderMappingUtils.PRODUCER)
            .setPayloadType(NodePayloadType.Order)
            .setPayload(
                OrderNodePayload.newBuilder()
                    .setId(orderId)
                    .setOrderTimestamp(order.getOrderTimestamp())
                    .setItemsCount(order.getOrderedItems().size())
                    .setTotalPrice(
                        order.getOrderedItems().stream()
                            .map(OrderedItem::getPrice)
                            .reduce(0.0, Double::sum))
                    .build())
            .build();
    nodes.add(new KeyValue<>(orderId, orderNode));

    String customerId = OrderMappingUtils.getCustomerId(order, order.getCustomer());
    DomainNode customerNode =
        DomainNode.newBuilder()
            .setId(customerId)
            .setProducer(OrderMappingUtils.PRODUCER)
            .setPayloadType(NodePayloadType.Customer)
            .setPayload(
                CustomerNodePayload.newBuilder()
                    .setName(order.getCustomer().getName())
                    .setAddress(order.getCustomer().getAddress())
                    .build())
            .build();
    nodes.add(new KeyValue<>(customerId, customerNode));
    for (var orderedItem : order.getOrderedItems()) {
      switch (orderedItem.getItemType()) {
        case book:
          BookOrder book = (BookOrder) orderedItem.getDetails();
          var bookId = OrderMappingUtils.getBookId(order, book);
          DomainNode bookNode =
              DomainNode.newBuilder()
                  .setId(bookId)
                  .setProducer(OrderMappingUtils.PRODUCER)
                  .setPayloadType(NodePayloadType.Book)
                  .setPayload(
                      BookNodePayload.newBuilder()
                          .setId(book.getId())
                          .setName(book.getName())
                          .setIsbn(book.getIsbn())
                          .setPrice(orderedItem.getPrice())
                          .build())
                  .build();
          nodes.add(new KeyValue<>(bookId, bookNode));
          break;
        case laptop:
          LaptopOrder laptop = (LaptopOrder) orderedItem.getDetails();
          var laptopId = OrderMappingUtils.getLaptopId(order, laptop);
          DomainNode laptopNode =
              DomainNode.newBuilder()
                  .setId(laptopId)
                  .setProducer(OrderMappingUtils.PRODUCER)
                  .setPayloadType(NodePayloadType.Laptop)
                  .setPayload(
                      LaptopNodePayload.newBuilder()
                          .setId(laptop.getId())
                          .setName(laptop.getName())
                          .setPrice(orderedItem.getPrice())
                          .build())
                  .build();
          nodes.add(new KeyValue<>(laptopId, laptopNode));
          break;
        default:
      }
    }
    return nodes;
  }
}
