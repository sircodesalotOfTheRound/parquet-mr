package org.apache.parquet.parqour.ingest.cursor.iterable;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iterators.IteratorReusingRollableRecordset;
import org.apache.parquet.parqour.ingest.cursor.iterators.RollableRecordSet;

import java.util.Iterator;

/**
 * Created by sircodesalot on 6/18/15.
 */
public final class Int32IterableCursor extends AdvanceableCursor implements Iterable<Integer> {
  private final RollableRecordSet<Integer> recordSet = new RollableRecordSet<Integer>(this);
  private final I32CursorIterator iterator;

  public Int32IterableCursor(String name, Integer[] array) {
    super(name);
    this.iterator = new I32CursorIterator(array);
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
  public RollableRecordSet<Integer> i32StartIteration(int headerIndex) {
    iterator.reset(headerIndex);
    return recordSet;
  }
}
