package ps.hassany.consistent.graph.orders.stream.mapping;

import io.vavr.Tuple;
import io.vavr.Tuple3;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.WindowStore;
import ps.hassany.consistent.graph.graph.Book;
import ps.hassany.consistent.graph.graph.Laptop;
import ps.hassany.consistent.graph.orders.BookOrder;
import ps.hassany.consistent.graph.orders.LaptopOrder;
import ps.hassany.consistent.graph.orders.Order;
import ps.hassany.consistent.graph.orders.OrderedItemType;
import ps.hassany.consistent.graph.orders.internal.*;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OrderDiffTransformer
    implements Transformer<String, Order, KeyValue<String, OrderWithState>> {
  private ProcessorContext context;
  private final String storeName;
  private WindowStore<String, Order> ordersWindowStore;
  private final long leftDurationMs;
  private final long rightDurationMs;

  public OrderDiffTransformer(
      final String storeName, final long leftDurationMs, final long rightDurationMs) {
    this.storeName = storeName;
    this.leftDurationMs = leftDurationMs;
    this.rightDurationMs = rightDurationMs;
  }

  @Override
  public void init(final ProcessorContext context) {
    this.context = context;
    ordersWindowStore = this.context.getStateStore(storeName);
  }

  private OrderWithState newOrder(Order order) {
    return OrderWithState.newBuilder()
        .setState(OrderState.Created)
        .setId(order.getId())
        .setOrderTimestamp(order.getOrderTimestamp())
        .setCustomer(
            CustomerOrderWithState.newBuilder()
                .setState(OrderState.Created)
                .setName(order.getCustomer().getName())
                .setAddress(order.getCustomer().getAddress())
                .build())
        .setOrderedItems(
            order.getOrderedItems().stream()
                .map(
                    orderedItem -> {
                      if (orderedItem.getItemType() == OrderedItemType.book) {
                        BookOrder book = (BookOrder) orderedItem.getDetails();
                        return OrderedItem.newBuilder()
                            .setItemType(
                                ps.hassany.consistent.graph.orders.internal.OrderedItemType.book)
                            .setPrice(orderedItem.getPrice())
                            .setDetails(
                                BookOrderWithState.newBuilder()
                                    .setState(OrderState.Created)
                                    .setId(book.getId())
                                    .setIsbn(book.getIsbn())
                                    .setName(book.getName())
                                    .build())
                            .build();
                      } else if (orderedItem.getItemType() == OrderedItemType.laptop) {
                        LaptopOrder laptop = (LaptopOrder) orderedItem.getDetails();
                        return OrderedItem.newBuilder()
                            .setItemType(
                                ps.hassany.consistent.graph.orders.internal.OrderedItemType.laptop)
                            .setPrice(orderedItem.getPrice())
                            .setDetails(
                                LaptopOrderWithState.newBuilder()
                                    .setState(OrderState.Created)
                                    .setId(laptop.getId())
                                    .setName(laptop.getName())
                                    .build())
                            .build();
                      }
                      return null;
                    })
                .collect(Collectors.toList()))
        .build();
  }

  private BookOrderWithState bookToBookWithState(Book book, OrderState state) {
    return BookOrderWithState.newBuilder()
        .setState(state)
        .setId(book.getId())
        .setIsbn(book.getIsbn())
        .setName(book.getName())
        .build();
  }

  private LaptopOrderWithState laptopToLaptopWithState(Laptop laptop, OrderState state) {
    return LaptopOrderWithState.newBuilder()
        .setState(state)
        .setId(laptop.getId())
        .setName(laptop.getName())
        .build();
  }

  private OrderedItem orderedItemToOrderedItemWithState(
      ps.hassany.consistent.graph.orders.OrderedItem orderedItem, OrderState state) {
    if (orderedItem.getItemType() == OrderedItemType.book) {
      Book book = (Book) orderedItem.getDetails();
      return OrderedItem.newBuilder()
          .setItemType(ps.hassany.consistent.graph.orders.internal.OrderedItemType.book)
          .setPrice(orderedItem.getPrice())
          .setDetails(bookToBookWithState(book, state))
          .build();
    } else if (orderedItem.getItemType() == OrderedItemType.laptop) {
      Laptop laptop = (Laptop) orderedItem.getDetails();
      return OrderedItem.newBuilder()
          .setItemType(ps.hassany.consistent.graph.orders.internal.OrderedItemType.laptop)
          .setPrice(orderedItem.getPrice())
          .setDetails(laptopToLaptopWithState(laptop, state))
          .build();
    }
    return null;
  }

  private String extractOrderedItemId(ps.hassany.consistent.graph.orders.OrderedItem orderedItem) {
    if (orderedItem.getItemType() == OrderedItemType.book) {
      Book book = (Book) orderedItem.getDetails();
      return book.getId();
    } else if (orderedItem.getItemType() == OrderedItemType.laptop) {
      Laptop laptop = (Laptop) orderedItem.getDetails();
      return laptop.getId();
    }
    return null;
  }

  private OrderWithState deleteOrder(Order oldOrder) {
    var orderWithSateBuilder =
        OrderWithState.newBuilder()
            .setState(OrderState.Updated)
            .setId(oldOrder.getId())
            .setOrderTimestamp(System.currentTimeMillis())
            .setCustomer(
                CustomerOrderWithState.newBuilder()
                    .setState(OrderState.Deleted)
                    .setName(oldOrder.getCustomer().getName())
                    .setAddress(oldOrder.getCustomer().getAddress())
                    .build());
    orderWithSateBuilder.setOrderedItems(
        oldOrder.getOrderedItems().stream()
            .map(orderedItem -> orderedItemToOrderedItemWithState(orderedItem, OrderState.Deleted))
            .collect(Collectors.toList()));
    return orderWithSateBuilder.build();
  }

  private OrderWithState orderDiff(Order oldOrder, Order newOrder) {
    if (newOrder == null) {
      return deleteOrder(oldOrder);
    }
    var orderWithSateBuilder =
        OrderWithState.newBuilder()
            .setState(OrderState.Updated)
            .setId(oldOrder.getId())
            .setOrderTimestamp(newOrder.getOrderTimestamp())
            .setCustomer(
                CustomerOrderWithState.newBuilder()
                    .setState(OrderState.Updated)
                    .setName(newOrder.getCustomer().getName())
                    .setAddress(newOrder.getCustomer().getAddress())
                    .build());

    var orderedItemsWithState =
        Stream.concat(
                // Assume all items in the old order are deleted
                oldOrder.getOrderedItems().stream()
                    .map(
                        orderedItem ->
                            Tuple.of(
                                extractOrderedItemId(orderedItem), OrderState.Deleted, orderedItem))
                    .collect(Collectors.toList())
                    .stream(),
                // Assume all items in the new order are Created
                newOrder.getOrderedItems().stream()
                    .map(
                        orderedItem ->
                            Tuple.of(
                                extractOrderedItemId(orderedItem), OrderState.Created, orderedItem))
                    .collect(Collectors.toList())
                    .stream())
            .collect(
                Collectors.toMap(
                    Tuple3::_1,
                    v -> Tuple.of(v._2(), v._3()),
                    // When there's two versions of the same item, take the new one with state
                    // 'Created' and update state to 'Updated'
                    (oldValue, newValue) ->
                        newValue._1() == OrderState.Created
                            ? Tuple.of(OrderState.Updated, newValue._2())
                            : Tuple.of(OrderState.Updated, oldValue._2())))
            .values()
            .stream()
            .map(value -> orderedItemToOrderedItemWithState(value._2(), value._1()))
            .collect(Collectors.toList());

    orderWithSateBuilder.setOrderedItems(orderedItemsWithState);
    return orderWithSateBuilder.build();
  }

  @Override
  public KeyValue<String, OrderWithState> transform(String key, Order order) {
    final long eventTimestamp = order.getOrderTimestamp();
    final var timeIterator =
        ordersWindowStore.fetch(
            key, eventTimestamp - leftDurationMs, eventTimestamp + rightDurationMs);
    OrderWithState orderWithState;
    if (timeIterator.hasNext()) {
      orderWithState = orderDiff(timeIterator.next().value, order);
    } else {
      orderWithState = newOrder(order);
    }
    ordersWindowStore.put(key, order, eventTimestamp);
    return new KeyValue<>(key, orderWithState);
  }

  @Override
  public void close() {}
}
