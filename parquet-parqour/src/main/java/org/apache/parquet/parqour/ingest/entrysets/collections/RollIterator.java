package org.apache.parquet.parqour.ingest.entrysets.collections;

import java.util.Iterator;

/**
 * Created by sircodesalot on 7/21/15.
 */
public class RollIterator<T> implements Iterator<T> {
  private int index = 0;
  private final int length;
  private final Object[] items;

  public RollIterator(Object[] items, int length) {
    this.items = items;
    this.length = length;
  }

  @Override
  public boolean hasNext() {
    return index < length;
  }

  @Override
  public T next() {
    return (T)items[index++];
  }

  public Iterator<T> reset() {
    this.index = 0;
    return this;
  }
}
