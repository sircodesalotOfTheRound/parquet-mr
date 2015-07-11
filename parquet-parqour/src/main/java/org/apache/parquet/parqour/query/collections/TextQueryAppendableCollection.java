package org.apache.parquet.parqour.query.collections;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by sircodesalot on 15/4/7.
 */
public class TextQueryAppendableCollection<T>  extends TextQueryCollection<T> {
  private final List<T> items = new ArrayList<T>();

  public TextQueryAppendableCollection() {
  }

  public TextQueryAppendableCollection(Iterable<T> items) {
    this.add(items);
  }

  public TextQueryAppendableCollection(T... items) {
    for (T item : items) {
      this.add(item);
    }
  }

  public TextQueryAppendableCollection add(T item) {
    this.items.add(item);
    return this;
  }

  public TextQueryAppendableCollection add(Iterable<T> items) {
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

