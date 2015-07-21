package org.apache.parquet.parqour.ingest.cursor.iterable.field;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.cursor.iterators.ResettableCursorIterator;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by sircodesalot on 7/13/15.
 */
public class GroupCursorIterator extends ResettableCursorIterator<Cursor> {
  private static final int FIRST_CHILD_COLUMN = 0;

  private final Integer[][] array;
  private final GroupIterIndexingCursor cursor;
  private int end;

  private int index;

  public GroupCursorIterator(String name, AdvanceableCursor[] children, Integer[][] schemaLinks) {
    this.cursor = new GroupIterIndexingCursor(name, children, schemaLinks);
    this.array = schemaLinks;
  }

  @Override
  public boolean hasNext() {
    return index < end;
  }

  @Override
  public Cursor next() {
    return cursor.advanceTo(index++);
  }

  @Override
  public void remove() {
    throw new NotImplementedException();
  }

  @Override
  public void reset(int start) {
    this.index = start + 1;
    this.end = index + array[FIRST_CHILD_COLUMN][start];
  }
}
