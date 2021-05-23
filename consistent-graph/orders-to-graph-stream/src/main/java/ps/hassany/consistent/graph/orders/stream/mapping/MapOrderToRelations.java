package ps.hassany.consistent.graph.orders.stream.mapping;

import org.apache.kafka.streams.KeyValue;
import ps.hassany.consistent.graph.domain.DomainRelation;
import ps.hassany.consistent.graph.orders.BookOrder;
import ps.hassany.consistent.graph.orders.LaptopOrder;
import ps.hassany.consistent.graph.orders.Order;
import ps.hassany.consistent.graph.orders.OrderedItem;

import java.util.LinkedList;
import java.util.List;

public class MapOrderToRelations
    implements KeyValueMappingSupplier<String, Order, String, DomainRelation> {
  @Override
  public List<KeyValue<String, DomainRelation>> map(String key, Order order) {
    List<KeyValue<String, DomainRelation>> Relations = new LinkedList<>();
    String orderId = "order_" + order.getId();

    for (Object item : order.getOrderedItems()) {
      OrderedItem orderedItem = (OrderedItem) item;
      DomainRelation rel;

      var customerId = OrderMappingUtils.getCustomerId(order, order.getCustomer());
      rel =
          DomainRelation.newBuilder()
              .setSource(orderId)
              .setType(OrderMappingUtils.ORDERED_BY)
              .setTarget(customerId)
              .build();
      Relations.add(
          new KeyValue<>(
              OrderMappingUtils.getRelId(orderId, OrderMappingUtils.ORDERED_BY, customerId), rel));
      switch (orderedItem.getItemType()) {
        case book:
          BookOrder book = (BookOrder) orderedItem.getDetails();
          var bookId = OrderMappingUtils.getBookId(order, book);
          rel =
              DomainRelation.newBuilder()
                  .setSource(orderId)
                  .setType(OrderMappingUtils.HAS_ITEM)
                  .setTarget(bookId)
                  .build();
          Relations.add(
              new KeyValue<>(
                  OrderMappingUtils.getRelId(orderId, OrderMappingUtils.HAS_ITEM, bookId), rel));
          break;
        case laptop:
          LaptopOrder laptop = (LaptopOrder) orderedItem.getDetails();
          var laptopId = OrderMappingUtils.getLaptopId(order, laptop);
          rel =
              DomainRelation.newBuilder()
                  .setSource(orderId)
                  .setType(OrderMappingUtils.HAS_ITEM)
                  .setTarget(laptopId)
                  .build();
          Relations.add(
              new KeyValue<>(
                  OrderMappingUtils.getRelId(orderId, OrderMappingUtils.HAS_ITEM, laptopId), rel));
          break;
        default:
      }
    }
    return Relations;
  }
}
