package ps.hassany.consistent.graph.orders.stream.mapping;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;
import ps.hassany.consistent.graph.orders.Order;
import ps.hassany.consistent.graph.orders.internal.DLQRecord;

import java.util.Set;

public class OrderDiffTransformerSupplier
    implements TransformerSupplier<String, Order, KeyValue<String, DLQRecord>> {

  private final String storeName;
  private final long leftDurationMs;
  private final long rightDurationMs;
  private final StoreBuilder<WindowStore<String, Order>> ordersStoreBuilder;

  public OrderDiffTransformerSupplier(
      final String storeName,
      final long maintainDurationPerEventInMs,
      StoreBuilder<WindowStore<String, Order>> ordersStoreBuilder) {
    this.storeName = storeName;
    leftDurationMs = maintainDurationPerEventInMs / 2;
    rightDurationMs = maintainDurationPerEventInMs - leftDurationMs;
    this.ordersStoreBuilder = ordersStoreBuilder;
  }

  @Override
  public Transformer<String, Order, KeyValue<String, DLQRecord>> get() {
    return new OrderDiffTransformer(storeName, leftDurationMs, rightDurationMs);
  }

  @Override
  public Set<StoreBuilder<?>> stores() {
    return Set.of(this.ordersStoreBuilder);
  }
}
