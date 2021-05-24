package ps.hassany.consistent.graph.orders.stream.mapping;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import ps.hassany.consistent.graph.NodeState;
import ps.hassany.consistent.graph.domain.DomainNode;
import ps.hassany.consistent.graph.domain.NodePayloadType;
import ps.hassany.consistent.graph.orders.internal.*;
import ps.hassany.consistent.graph.orders.payload.BookNodePayload;
import ps.hassany.consistent.graph.orders.payload.CustomerNodePayload;
import ps.hassany.consistent.graph.orders.payload.LaptopNodePayload;
import ps.hassany.consistent.graph.orders.payload.OrderNodePayload;

import java.util.LinkedList;
import java.util.List;

public class MapOrderToNodes
    implements KeyValueMapper<String, OrderWithState, Iterable<KeyValue<String, DomainNode>>> {

  private DomainNode getOrderNode(OrderWithState order) {
    String orderId = OrderMappingUtils.getOrderId(order);
    if (order.getState() == OrderState.Deleted) {
      return DomainNode.newBuilder()
          .setId(orderId)
          .setProducer(OrderMappingUtils.PRODUCER)
          .setPayloadType(NodePayloadType.Order)
          .setState(NodeState.Deleted)
          .setPayload(
              OrderNodePayload.newBuilder()
                  .setId(orderId)
                  .setOrderTimestamp(System.currentTimeMillis())
                  .setItemsCount(0)
                  .setTotalPrice(0)
                  .build())
          .build();
    } else {
      return DomainNode.newBuilder()
          .setId(orderId)
          .setProducer(OrderMappingUtils.PRODUCER)
          .setPayloadType(NodePayloadType.Order)
          .setState(OrderMappingUtils.nodeStateFromOrderState(order.getState()))
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
    }
  }

  private DomainNode getCustomerNode(String customerId, CustomerOrderWithState customer) {
    if (customer.getState() == OrderState.Deleted) {
      return DomainNode.newBuilder()
          .setId(customerId)
          .setProducer(OrderMappingUtils.PRODUCER)
          .setState(NodeState.Deleted)
          .setPayloadType(NodePayloadType.Customer)
          .setPayload(CustomerNodePayload.newBuilder().setName("EMPTY").setAddress("EMPTY").build())
          .build();
    } else {
      return DomainNode.newBuilder()
          .setId(customerId)
          .setProducer(OrderMappingUtils.PRODUCER)
          .setState(OrderMappingUtils.nodeStateFromOrderState(customer.getState()))
          .setPayloadType(NodePayloadType.Customer)
          .setPayload(
              CustomerNodePayload.newBuilder()
                  .setName(customer.getName())
                  .setAddress(customer.getAddress())
                  .build())
          .build();
    }
  }

  private DomainNode getBookNode(String bookId, OrderedItem orderedItem, BookOrderWithState book) {
    return DomainNode.newBuilder()
        .setId(bookId)
        .setProducer(OrderMappingUtils.PRODUCER)
        .setState(OrderMappingUtils.nodeStateFromOrderState(book.getState()))
        .setPayloadType(NodePayloadType.Book)
        .setPayload(
            BookNodePayload.newBuilder()
                .setId(book.getId())
                .setName(book.getName())
                .setIsbn(book.getIsbn())
                .setPrice(orderedItem.getPrice())
                .build())
        .build();
  }

  private DomainNode getLaptopNode(
      String laptopId, OrderedItem orderedItem, LaptopOrderWithState laptop) {

    return DomainNode.newBuilder()
        .setId(laptopId)
        .setProducer(OrderMappingUtils.PRODUCER)
        .setState(OrderMappingUtils.nodeStateFromOrderState(laptop.getState()))
        .setPayloadType(NodePayloadType.Laptop)
        .setPayload(
            LaptopNodePayload.newBuilder()
                .setId(laptop.getId())
                .setName(laptop.getName())
                .setPrice(orderedItem.getPrice())
                .build())
        .build();
  }

  @Override
  public Iterable<KeyValue<String, DomainNode>> apply(String key, OrderWithState order) {
    List<KeyValue<String, DomainNode>> nodes = new LinkedList<>();

    String orderId = OrderMappingUtils.getOrderId(order);
    DomainNode orderNode = getOrderNode(order);
    nodes.add(new KeyValue<>(orderId, orderNode));

    CustomerOrderWithState customer = order.getCustomer();
    String customerId = OrderMappingUtils.getCustomerId(order, customer);
    var customerNode = getCustomerNode(customerId, customer);
    nodes.add(new KeyValue<>(customerId, customerNode));

    for (var orderedItem : order.getOrderedItems()) {
      switch (orderedItem.getItemType()) {
        case book:
          BookOrderWithState book = (BookOrderWithState) orderedItem.getDetails();
          var bookId = OrderMappingUtils.getBookId(order, book);
          nodes.add(new KeyValue<>(bookId, getBookNode(bookId, orderedItem, book)));
          break;
        case laptop:
          LaptopOrderWithState laptop = (LaptopOrderWithState) orderedItem.getDetails();
          var laptopId = OrderMappingUtils.getLaptopId(order, laptop);
          nodes.add(new KeyValue<>(laptopId, getLaptopNode(laptopId, orderedItem, laptop)));
          break;
      }
    }
    return nodes;
  }
}
