package org.apache.parquet.parqour.ingest.entrysets.collections;

import org.apache.parquet.parqour.cursor.iterators.RollableFieldEntries;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by sircodesalot on 7/5/15.
 */
public class Roll<T> extends RollableFieldEntries<T> {
  private final Object[] items;
  private int count;
  private final RollIterator<T> iterator;

  public Roll(Iterable<T> iterable) {
    super(iterable);

    this.items = addAll(iterable);
    this.iterator = new RollIterator<T>(items, count);
  }

  private Object[] addAll(Iterable<T> iterable) {
    Object[] results = new Object[4];

    count = 0;
    for (T item : iterable) {
      if (count >= results.length) {
        results = Arrays.copyOf(results, results.length * 2);
      }

      results[count++] = item;
    }

    return results;
  }

  @Override
  public int count() {
    return this.count;
  }

  @Override
  public Iterator<T> iterator() {
    return iterator.reset();
  }
}
