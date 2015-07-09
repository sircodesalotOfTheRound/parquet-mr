package org.apache.parquet.parqour.query.collections;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sircodesalot on 15/4/7.
 */
public class ParquelAppendableCollection<T>  extends ParquelCollection<T> {
  private final List<T> items = new ArrayList<T>();

  public ParquelAppendableCollection() {
  }

  public ParquelAppendableCollection(Iterable<T> items) {
    this.add(items);
  }

  public ParquelAppendableCollection(T... items) {
    for (T item : items) {
      this.add(item);
    }
  }

  public ParquelAppendableCollection add(T item) {
    this.items.add(item);
    return this;
  }

  public ParquelAppendableCollection add(Iterable<T> items) {
    for (T item : items) {
      this.add(item);
    }

    return this;
  }

  @Override
  public Iterable<T> items() {
    return items;
  }
}

