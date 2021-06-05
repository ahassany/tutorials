package ps.hassany.consistent.graph.orders.stream.mapping;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import ps.hassany.consistent.graph.NodeState;
import ps.hassany.consistent.graph.domain.*;
import ps.hassany.consistent.graph.orders.internal.*;
import ps.hassany.consistent.graph.orders.payload.BookNodePayload;
import ps.hassany.consistent.graph.orders.payload.CustomerNodePayload;
import ps.hassany.consistent.graph.orders.payload.LaptopNodePayload;
import ps.hassany.consistent.graph.orders.payload.OrderNodePayload;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapOrderToGraphRecord
    implements KeyValueMapper<
        String, OrderWithState, Iterable<KeyValue<String, DomainGraphRecord>>> {

  private DomainGraphRecord getOrderRecord(OrderWithState order) {
    String orderId = OrderMappingUtils.getOrderId(order);
    DomainNode.Builder orderNodeBuilder;
    if (order.getState() == OrderState.Deleted) {
      orderNodeBuilder =
          DomainNode.newBuilder()
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
                      .build());
    } else {
      orderNodeBuilder =
          DomainNode.newBuilder()
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
                      .build());
    }
    return DomainGraphRecord.newBuilder()
        .setPayloadType(DomainContainerPayloadType.DomainNode)
        .setPayload(orderNodeBuilder.build())
        .build();
  }

  private DomainGraphRecord getCustomerNodeRecord(
      String customerId, CustomerOrderWithState customer) {
    DomainNode customerNode;
    if (customer.getState() == OrderState.Deleted) {
      customerNode =
          DomainNode.newBuilder()
              .setId(customerId)
              .setProducer(OrderMappingUtils.PRODUCER)
              .setState(NodeState.Deleted)
              .setPayloadType(NodePayloadType.Customer)
              .setPayload(
                  CustomerNodePayload.newBuilder().setName("EMPTY").setAddress("EMPTY").build())
              .build();
    } else {
      customerNode =
          DomainNode.newBuilder()
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
    return DomainGraphRecord.newBuilder()
        .setPayloadType(DomainContainerPayloadType.DomainNode)
        .setPayload(customerNode)
        .build();
  }

  private DomainGraphRecord getBookNodeRecord(
      String bookId, OrderedItem orderedItem, BookOrderWithState book) {
    DomainNode bookNode =
        DomainNode.newBuilder()
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
    return DomainGraphRecord.newBuilder()
        .setPayloadType(DomainContainerPayloadType.DomainNode)
        .setPayload(bookNode)
        .build();
  }

  private DomainGraphRecord getLaptopNode(
      String laptopId, OrderedItem orderedItem, LaptopOrderWithState laptop) {
    DomainNode laptopNode =
        DomainNode.newBuilder()
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
    return DomainGraphRecord.newBuilder()
        .setPayloadType(DomainContainerPayloadType.DomainNode)
        .setPayload(laptopNode)
        .build();
  }

  private Tuple2<String, DomainGraphRecord> extractOrderedItemRelState(
      OrderWithState order, OrderedItem orderedItem) {
    String orderId = OrderMappingUtils.getOrderId(order);
    if (orderedItem.getItemType() == OrderedItemType.book) {
      var book = (BookOrderWithState) orderedItem.getDetails();
      var bookId = OrderMappingUtils.getBookId(order, book);
      return Tuple.of(
          OrderMappingUtils.getRelId(orderId, OrderMappingUtils.HAS_ITEM, bookId),
          DomainGraphRecord.newBuilder()
              .setPayloadType(DomainContainerPayloadType.DomainRelation)
              .setPayload(
                  DomainRelation.newBuilder()
                      .setState(getRelState(order.getState(), book.getState()))
                      .setSource(orderId)
                      .setType(OrderMappingUtils.HAS_ITEM)
                      .setTarget(bookId)
                      .build())
              .build());
    } else if (orderedItem.getItemType() == OrderedItemType.laptop) {
      var laptop = (LaptopOrderWithState) orderedItem.getDetails();
      var laptopId = OrderMappingUtils.getLaptopId(order, laptop);
      return Tuple.of(
          OrderMappingUtils.getRelId(orderId, OrderMappingUtils.HAS_ITEM, laptopId),
          DomainGraphRecord.newBuilder()
              .setPayloadType(DomainContainerPayloadType.DomainRelation)
              .setPayload(
                  DomainRelation.newBuilder()
                      .setState(getRelState(order.getState(), laptop.getState()))
                      .setSource(orderId)
                      .setType(OrderMappingUtils.HAS_ITEM)
                      .setTarget(laptopId)
                      .build())
              .build());
    }
    return null;
  }

  private NodeState getRelState(OrderState orderState, OrderState itemState) {
    if (orderState == OrderState.Deleted || itemState == OrderState.Deleted) {
      return NodeState.Deleted;
    } else {
      return OrderMappingUtils.nodeStateFromOrderState(itemState);
    }
  }

  public List<KeyValue<String, DomainGraphRecord>> getNodeRecords(OrderWithState order) {
    List<KeyValue<String, DomainGraphRecord>> nodes = new LinkedList<>();

    String orderId = OrderMappingUtils.getOrderId(order);
    DomainGraphRecord orderNodeRecord = getOrderRecord(order);
    nodes.add(new KeyValue<>(orderId, orderNodeRecord));

    CustomerOrderWithState customer = order.getCustomer();
    String customerId = OrderMappingUtils.getCustomerId(order, customer);
    var customerNodeRecord = getCustomerNodeRecord(customerId, customer);
    nodes.add(new KeyValue<>(customerId, customerNodeRecord));

    for (var orderedItem : order.getOrderedItems()) {
      switch (orderedItem.getItemType()) {
        case book:
          BookOrderWithState book = (BookOrderWithState) orderedItem.getDetails();
          var bookId = OrderMappingUtils.getBookId(order, book);
          nodes.add(new KeyValue<>(bookId, getBookNodeRecord(bookId, orderedItem, book)));
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

  public List<KeyValue<String, DomainGraphRecord>> getRelationsRecords(OrderWithState order) {
    var orderId = OrderMappingUtils.getOrderId(order);
    var customer = order.getCustomer();
    var customerId = OrderMappingUtils.getCustomerId(order, customer);

    var customerRel =
        List.of(
            Tuple.of(
                OrderMappingUtils.getRelId(orderId, OrderMappingUtils.ORDERED_BY, customerId),
                DomainGraphRecord.newBuilder()
                    .setPayloadType(DomainContainerPayloadType.DomainRelation)
                    .setPayload(
                        DomainRelation.newBuilder()
                            .setState(getRelState(order.getState(), customer.getState()))
                            .setSource(orderId)
                            .setType(OrderMappingUtils.ORDERED_BY)
                            .setTarget(customerId)
                            .build())
                    .build()));

    return Stream.concat(
            customerRel.stream(),
            order.getOrderedItems().stream()
                .map(orderedItem -> extractOrderedItemRelState(order, orderedItem)))
        .filter(Objects::nonNull)
        .map(relState -> new KeyValue<>(relState._1(), relState._2()))
        .collect(Collectors.toList());
  }

  @Override
  public Iterable<KeyValue<String, DomainGraphRecord>> apply(String key, OrderWithState value) {
    List<KeyValue<String, DomainGraphRecord>> records = new LinkedList<>();
    records.addAll(getNodeRecords(value));
    records.addAll(
        getRelationsRecords(value).stream().filter(Objects::nonNull).collect(Collectors.toList()));

    records.forEach(x -> {System.err.println("Writing" + x);});
    return records;
  }
}
