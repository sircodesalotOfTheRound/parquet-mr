package org.apache.parquet.parqour.tools;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sircodesalot on 15/4/7.
 */
public class TransformList<T>  extends TransformCollection<T> {
  private final List<T> items = new ArrayList<T>();

  public TransformList() {
  }

  public TransformList(Iterable<T> items) {
    this.add(items);
  }

  public TransformList(T... items) {
    for (T item : items) {
      this.add(item);
    }
  }

  public TransformList<T> add(T item) {
    this.items.add(item);
    return this;
  }

  public TransformList<T> add(Iterable<T> items) {
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

