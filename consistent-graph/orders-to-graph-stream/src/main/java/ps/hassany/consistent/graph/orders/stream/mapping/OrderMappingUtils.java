package ps.hassany.consistent.graph.orders.stream.mapping;



import ps.hassany.consistent.graph.orders.BookOrder;
import ps.hassany.consistent.graph.orders.CustomerOrder;
import ps.hassany.consistent.graph.orders.LaptopOrder;
import ps.hassany.consistent.graph.orders.Order;

import java.util.List;

public class OrderMappingUtils {
  public static final String PRODUCER = "ordersDepartment";
  public static final String HAS_ITEM = "HAS_ITEM";
  public static final String ORDERED_BY = "ORDERED_BY";
  private static final String PREFIX = "order_";

  public static String getOrderId(Order order) {
    return PREFIX + order.getId();
  }

  public static String getCustomerId(Order order, CustomerOrder customer) {
    return getOrderId(order) + "_customer_" + customer.getName();
  }

  public static String getBookId(Order order, BookOrder book) {
    return getOrderId(order) + "_book_" + book.getId();
  }

  public static String getLaptopId(Order order, LaptopOrder laptop) {
    return getOrderId(order) + "_laptop_" + laptop.getId();
  }

  public static String getRelId(String source, String type, String target) {
    return String.join("_", List.of(source, type, target));
  }
}
