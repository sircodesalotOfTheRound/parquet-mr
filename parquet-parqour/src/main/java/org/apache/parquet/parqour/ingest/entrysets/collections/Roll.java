package org.apache.parquet.parqour.ingest.entrysets.collections;

import org.apache.parquet.parqour.ingest.cursor.iterators.RollableFieldEntries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sircodesalot on 7/5/15.
 */
public class Roll<T> extends RollableFieldEntries<T> {
  private final RollIterator<T> iterator;

  public Roll(Iterable<T> iterable) {
    super(iterable);
    this.iterator = new RollIterator<T>(addAll(iterable));
  }

  private Object[] addAll(Iterable<T> iterable) {
    Object[] results = new Object[4];

    int index = 0;
    for (T item : iterable) {
      if (index >= results.length) {
        results = Arrays.copyOf(results, results.length * 2);
      }

      results[index++] = item;
    }

    return results;
  }

  @Override
  public Iterator<T> iterator() {
    return iterator.reset();
  }
}
