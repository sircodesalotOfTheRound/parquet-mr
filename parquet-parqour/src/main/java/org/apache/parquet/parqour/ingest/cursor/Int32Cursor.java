package org.apache.parquet.parqour.ingest.cursor;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

/**
 * Created by sircodesalot on 6/18/15.
 */
public final class Int32Cursor extends AdvanceableCursor {
  private final Integer[] array;

  private static class I32CursorIterator implements Iterator<Integer> {
    private final Integer[] array;
    private final int end;

    private int index;
    public I32CursorIterator(Integer[] array, int start, int end) {
      this.array = array;
      this.index = start;
      this.end = end;

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
  }

  public Int32Cursor(String name, Integer[] array) {
    super(name);
    this.array = array;
  }

  @Override
  public Integer i32() {
    return array[start];
  }

  @Override
  public Object value() {
    return array[start];
  }
}
