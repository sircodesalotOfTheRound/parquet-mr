package org.apache.parquet.parqour.ingest.cursor.iterable.aggregation;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.cursor.iterators.ResettableCursorIterator;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 7/13/15.
 */
public class GroupCursorIterator extends ResettableCursorIterator<Cursor> {
  private final Integer[] array;
  private final AdvanceableCursor cursor;
  private int end;

  private int index;

  public GroupCursorIterator(AdvanceableCursor cursor, Integer[] array) {
    this.cursor = cursor;
    this.array = array;
  }

  @Override
  public boolean hasNext() {
    return index < end;
  }

  @Override
  public Cursor next() {
    int next = array[index++];
    return this.cursor.advanceTo(next);
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
