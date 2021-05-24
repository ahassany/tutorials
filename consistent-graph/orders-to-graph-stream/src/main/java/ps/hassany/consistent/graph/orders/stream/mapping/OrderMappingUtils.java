package ps.hassany.consistent.graph.orders.stream.mapping;

import ps.hassany.consistent.graph.NodeState;
import ps.hassany.consistent.graph.orders.internal.*;

import java.util.List;

public class OrderMappingUtils {
  public static final String PRODUCER = "ordersDepartment";
  public static final String HAS_ITEM = "HAS_ITEM";
  public static final String ORDERED_BY = "ORDERED_BY";
  private static final String PREFIX = "order_";

  public static NodeState nodeStateFromOrderState(OrderState orderState) {
    switch (orderState) {
      case Created:
        return NodeState.Created;
      case Deleted:
        return NodeState.Deleted;
      case Updated:
        return NodeState.Updated;
      default:
        return null;
    }
  }

  public static String getOrderId(OrderWithState order) {
    return PREFIX + order.getId();
  }

  public static String getCustomerId(OrderWithState order, CustomerOrderWithState customer) {
    return getOrderId(order) + "_customer_" + customer.getName();
  }

  public static String getBookId(OrderWithState order, BookOrderWithState book) {
    return getOrderId(order) + "_book_" + book.getId();
  }

  public static String getLaptopId(OrderWithState order, LaptopOrderWithState laptop) {
    return getOrderId(order) + "_laptop_" + laptop.getId();
  }

  public static String getRelId(String source, String type, String target) {
    return String.join("_", List.of(source, type, target));
  }
}
