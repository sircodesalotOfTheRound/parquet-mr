package org.apache.parquet.parqour.ingest.cursor.iterable.aggregation;

import org.apache.parquet.parqour.ingest.cursor.iterators.ResettableCursorIterator;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 7/13/15.
 */
public class GroupCursorIterator extends ResettableCursorIterator<Integer> {
  private final Integer[] array;
  private int end;

  private int index;

  public GroupCursorIterator(Integer[] array) {
    this.array = array;
  }

  @Override
  public boolean hasNext() {
    return index < end;
  }

  @Override
  public Integer next() {
    return array[index++];
  }

  @Override
  public void remove() {
    throw new NotImplementedException();
  }

  @Override
  public void reset(int start) {
    this.index = start + 1;
    this.end = index + array[start];
  }
}
