package org.apache.parquet.parqour.ingest.cursor.implementations.iterable.i32;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iterators.RollableFieldEntries;

import java.util.Iterator;

/**
 * Created by sircodesalot on 6/18/15.
 */
public final class Int32IterableCursor extends AdvanceableCursor implements Iterable<Integer> {
  private final RollableFieldEntries<Integer> recordSet = new RollableFieldEntries<Integer>(this);
  private final I32CursorIterator iterator;

  public Int32IterableCursor(String name, int columnIndex, Integer[] array) {
    super(name, columnIndex);
    this.iterator = new I32CursorIterator(array);
  }

  public void setArray(Integer[] array) {
    this.iterator.setArray(array);
  }

  @Override
  public Object value() {
    return this.recordSet.roll();
  }

  @Override
  public Iterator<Integer> iterator() {
    return iterator;
  }

  @Override
  public RollableFieldEntries<Integer> i32StartIteration(int headerIndex) {
    iterator.reset(headerIndex);
    return recordSet;
  }
}
