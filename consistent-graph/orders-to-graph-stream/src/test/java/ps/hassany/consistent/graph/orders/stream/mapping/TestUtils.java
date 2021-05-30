package ps.hassany.consistent.graph.orders.stream.mapping;

import ps.hassany.consistent.graph.orders.OrderedItem;
import ps.hassany.consistent.graph.orders.OrderedItemType;
import ps.hassany.consistent.graph.orders.*;
import ps.hassany.consistent.graph.orders.internal.*;

import java.util.List;

public class TestUtils {
  public static Order getOrder1(long orderTimestamp) {
    return Order.newBuilder()
        .setId("order1")
        .setCustomer(CustomerOrder.newBuilder().setName("Customer1").setAddress("Address1").build())
        .setOrderTimestamp(orderTimestamp)
        .setOrderedItems(
            List.of(
                OrderedItem.newBuilder()
                    .setItemType(OrderedItemType.book)
                    .setPrice(10)
                    .setDetails(
                        BookOrder.newBuilder()
                            .setId("book1")
                            .setIsbn("isbn:1")
                            .setName("Book 1")
                            .build())
                    .build(),
                OrderedItem.newBuilder()
                    .setItemType(OrderedItemType.laptop)
                    .setPrice(100)
                    .setDetails(
                        LaptopOrder.newBuilder().setId("laptop1").setName("Laptop 1").build())
                    .build()))
        .build();
  }

  public static Order getOrder1WithoutBook(long orderTimestamp) {
    return Order.newBuilder()
        .setId("order1")
        .setCustomer(CustomerOrder.newBuilder().setName("Customer1").setAddress("Address1").build())
        .setOrderTimestamp(orderTimestamp)
        .setOrderedItems(
            List.of(
                OrderedItem.newBuilder()
                    .setItemType(OrderedItemType.laptop)
                    .setPrice(100)
                    .setDetails(
                        LaptopOrder.newBuilder().setId("laptop1").setName("Laptop 1").build())
                    .build()))
        .build();
  }

  public static Order getOrder1WithTwoBooks(long orderTimestamp) {
    return Order.newBuilder()
        .setId("order1")
        .setCustomer(CustomerOrder.newBuilder().setName("Customer1").setAddress("Address1").build())
        .setOrderTimestamp(orderTimestamp)
        .setOrderedItems(
            List.of(
                OrderedItem.newBuilder()
                    .setItemType(OrderedItemType.book)
                    .setPrice(10)
                    .setDetails(
                        BookOrder.newBuilder()
                            .setId("book1")
                            .setIsbn("isbn:1")
                            .setName("Book 1")
                            .build())
                    .build(),
                OrderedItem.newBuilder()
                    .setItemType(OrderedItemType.book)
                    .setPrice(10)
                    .setDetails(
                        BookOrder.newBuilder()
                            .setId("book2")
                            .setIsbn("isbn:2")
                            .setName("Book 2")
                            .build())
                    .build(),
                OrderedItem.newBuilder()
                    .setItemType(OrderedItemType.laptop)
                    .setPrice(100)
                    .setDetails(
                        LaptopOrder.newBuilder().setId("laptop1").setName("Laptop 1").build())
                    .build()))
        .build();
  }

  public static OrderWithState getOrder1CreateState(long orderTimestamp) {
    var order = getOrder1(orderTimestamp);
    var customer = order.getCustomer();
    BookOrder book = (BookOrder) order.getOrderedItems().get(0).getDetails();
    LaptopOrder laptop = (LaptopOrder) order.getOrderedItems().get(1).getDetails();
    return OrderWithState.newBuilder()
        .setState(OrderState.Created)
        .setId(order.getId())
        .setOrderTimestamp(order.getOrderTimestamp())
        .setCustomer(
            CustomerOrderWithState.newBuilder()
                .setState(OrderState.Created)
                .setName(customer.getName())
                .setAddress(customer.getAddress())
                .build())
        .setOrderedItems(
            List.of(
                ps.hassany.consistent.graph.orders.internal.OrderedItem.newBuilder()
                    .setPrice(order.getOrderedItems().get(0).getPrice())
                    .setItemType(ps.hassany.consistent.graph.orders.internal.OrderedItemType.book)
                    .setDetails(
                        BookOrderWithState.newBuilder()
                            .setState(OrderState.Created)
                            .setId(book.getId())
                            .setName(book.getName())
                            .setIsbn(book.getIsbn())
                            .build())
                    .build(),
                ps.hassany.consistent.graph.orders.internal.OrderedItem.newBuilder()
                    .setItemType(ps.hassany.consistent.graph.orders.internal.OrderedItemType.laptop)
                    .setPrice(order.getOrderedItems().get(1).getPrice())
                    .setItemType(ps.hassany.consistent.graph.orders.internal.OrderedItemType.laptop)
                    .setDetails(
                        LaptopOrderWithState.newBuilder()
                            .setState(OrderState.Created)
                            .setId(laptop.getId())
                            .setName(laptop.getName())
                            .build())
                    .build()))
        .build();
  }

  public static Order getOrder2(long orderTimestamp) {
    return Order.newBuilder()
        .setId("order2")
        .setCustomer(CustomerOrder.newBuilder().setName("Customer2").setAddress("Address2").build())
        .setOrderTimestamp(orderTimestamp)
        .setOrderedItems(
            List.of(
                OrderedItem.newBuilder()
                    .setItemType(OrderedItemType.book)
                    .setPrice(10)
                    .setDetails(
                        BookOrder.newBuilder()
                            .setId("book2")
                            .setIsbn("isbn:2")
                            .setName("Book 2")
                            .build())
                    .build(),
                OrderedItem.newBuilder()
                    .setItemType(OrderedItemType.laptop)
                    .setPrice(100)
                    .setDetails(
                        LaptopOrder.newBuilder().setId("laptop2").setName("Laptop 2").build())
                    .build()))
        .build();
  }

  public static OrderWithState getOrder2CreateState(long orderTimestamp) {
    var order = getOrder2(orderTimestamp);
    var customer = order.getCustomer();
    BookOrder book = (BookOrder) order.getOrderedItems().get(0).getDetails();
    LaptopOrder laptop = (LaptopOrder) order.getOrderedItems().get(1).getDetails();
    return OrderWithState.newBuilder()
        .setState(OrderState.Created)
        .setId(order.getId())
        .setOrderTimestamp(order.getOrderTimestamp())
        .setCustomer(
            CustomerOrderWithState.newBuilder()
                .setState(OrderState.Created)
                .setName(customer.getName())
                .setAddress(customer.getAddress())
                .build())
        .setOrderedItems(
            List.of(
                ps.hassany.consistent.graph.orders.internal.OrderedItem.newBuilder()
                    .setPrice(order.getOrderedItems().get(0).getPrice())
                    .setItemType(ps.hassany.consistent.graph.orders.internal.OrderedItemType.book)
                    .setDetails(
                        BookOrderWithState.newBuilder()
                            .setState(OrderState.Created)
                            .setId(book.getId())
                            .setName(book.getName())
                            .setIsbn(book.getIsbn())
                            .build())
                    .build(),
                ps.hassany.consistent.graph.orders.internal.OrderedItem.newBuilder()
                    .setItemType(ps.hassany.consistent.graph.orders.internal.OrderedItemType.laptop)
                    .setPrice(order.getOrderedItems().get(1).getPrice())
                    .setItemType(ps.hassany.consistent.graph.orders.internal.OrderedItemType.laptop)
                    .setDetails(
                        LaptopOrderWithState.newBuilder()
                            .setState(OrderState.Created)
                            .setId(laptop.getId())
                            .setName(laptop.getName())
                            .build())
                    .build()))
        .build();
  }

  public static OrderWithState order1Deleted(long orderTimestamp) {
    var order = getOrder1(orderTimestamp);
    return OrderWithState.newBuilder()
        .setState(OrderState.Deleted)
        .setId(order.getId())
        .setOrderTimestamp(order.getOrderTimestamp())
        .setOrderedItems(List.of())
        .build();
  }

  public static OrderWithState order1DeletedChildren(long orderTimestamp) {
    var order = getOrder1(orderTimestamp);
    var customer = order.getCustomer();
    var book = (BookOrder) order.getOrderedItems().get(0).getDetails();
    var laptop = (LaptopOrder) order.getOrderedItems().get(1).getDetails();
    return OrderWithState.newBuilder()
        .setState(OrderState.Deleted)
        .setId(order.getId())
        .setOrderTimestamp(order.getOrderTimestamp())
        .setCustomer(
            CustomerOrderWithState.newBuilder()
                .setState(OrderState.Deleted)
                .setName(customer.getName())
                .setAddress(customer.getAddress())
                .build())
        .setOrderedItems(
            List.of(
                ps.hassany.consistent.graph.orders.internal.OrderedItem.newBuilder()
                    .setPrice(order.getOrderedItems().get(0).getPrice())
                    .setItemType(ps.hassany.consistent.graph.orders.internal.OrderedItemType.book)
                    .setDetails(
                        BookOrderWithState.newBuilder()
                            .setState(OrderState.Deleted)
                            .setId(book.getId())
                            .setName(book.getName())
                            .setIsbn(book.getIsbn())
                            .build())
                    .build(),
                ps.hassany.consistent.graph.orders.internal.OrderedItem.newBuilder()
                    .setPrice(order.getOrderedItems().get(1).getPrice())
                    .setItemType(ps.hassany.consistent.graph.orders.internal.OrderedItemType.laptop)
                    .setDetails(
                        LaptopOrderWithState.newBuilder()
                            .setState(OrderState.Deleted)
                            .setId(laptop.getId())
                            .setName(laptop.getName())
                            .build())
                    .build()))
        .build();
  }

  public static DLQRecord wrapOrderStateInDLQ(
      OrderWithState orderWithStateState, long timestamp, boolean isError, String error) {
    return DLQRecord.newBuilder()
        .setTimestamp(timestamp)
        .setIsError(isError)
        .setError(error)
        .setOrderWithState(orderWithStateState)
        .build();
  }

  public static DLQRecord wrapOrderStateInDLQ(OrderWithState orderWithStateState, long timestamp) {
    return wrapOrderStateInDLQ(orderWithStateState, timestamp, false, null);
  }
}
