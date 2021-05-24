package ps.hassany.consistent.graph.orders.stream.mapping;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import ps.hassany.consistent.graph.NodeState;
import ps.hassany.consistent.graph.domain.DomainRelation;
import ps.hassany.consistent.graph.orders.internal.*;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MapOrderToRelations
    implements KeyValueMapper<String, OrderWithState, Iterable<KeyValue<String, DomainRelation>>> {

  private Tuple2<String, DomainRelation> extractOrderedItemRelState(
      OrderWithState order, OrderedItem orderedItem) {

    String orderId = OrderMappingUtils.getOrderId(order);
    if (orderedItem.getItemType() == OrderedItemType.book) {

      var book = (BookOrderWithState) orderedItem.getDetails();
      var bookId = OrderMappingUtils.getBookId(order, book);
      return Tuple.of(
          OrderMappingUtils.getRelId(orderId, OrderMappingUtils.HAS_ITEM, bookId),
          DomainRelation.newBuilder()
              .setState(getRelState(order.getState(), book.getState()))
              .setSource(orderId)
              .setType(OrderMappingUtils.HAS_ITEM)
              .setTarget(bookId)
              .build());
    } else if (orderedItem.getItemType() == OrderedItemType.laptop) {
      var laptop = (LaptopOrderWithState) orderedItem.getDetails();
      var laptopId = OrderMappingUtils.getLaptopId(order, laptop);
      return Tuple.of(
          OrderMappingUtils.getRelId(orderId, OrderMappingUtils.HAS_ITEM, laptopId),
          DomainRelation.newBuilder()
              .setState(getRelState(order.getState(), laptop.getState()))
              .setSource(orderId)
              .setType(OrderMappingUtils.HAS_ITEM)
              .setTarget(laptopId)
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

  @Override
  public List<KeyValue<String, DomainRelation>> apply(String key, OrderWithState order) {
    var orderId = OrderMappingUtils.getOrderId(order);
    var customer = order.getCustomer();
    var customerId = OrderMappingUtils.getCustomerId(order, customer);

    var customerRel =
        List.of(
            Tuple.of(
                OrderMappingUtils.getRelId(orderId, OrderMappingUtils.ORDERED_BY, customerId),
                DomainRelation.newBuilder()
                    .setState(getRelState(order.getState(), customer.getState()))
                    .setSource(orderId)
                    .setType(OrderMappingUtils.ORDERED_BY)
                    .setTarget(customerId)
                    .build()));

    return Stream.concat(
            customerRel.stream(),
            order.getOrderedItems().stream()
                .map(orderedItem -> extractOrderedItemRelState(order, orderedItem)))
        .filter(Objects::nonNull)
        .map(relState -> new KeyValue<>(relState._1(), relState._2()))
        .collect(Collectors.toList());
  }
}
