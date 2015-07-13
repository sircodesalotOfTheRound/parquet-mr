package org.apache.parquet.parqour.ingest.cursor;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import org.apache.parquet.parqour.ingest.cursor.iterators.RollableRecordSet;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

/**
 * Created by sircodesalot on 6/18/15.
 */
public final class Int32IterableCursor extends AdvanceableCursor implements Iterable<Integer> {
  private final Integer[] array;

  private static class I32CursorIterator implements Iterator<Integer> {
    private final Integer[] array;
    private int end;

    private int index;
    public I32CursorIterator(Integer[] array, int start) {
      this.array = array;
      this.index = start + 1;
      this.end = index + array[start];
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

  private Int32IterableCursor() {
    super("null_i32_iter_cursor");
    this.array = initializeZeroLengthList();
  }

  private Integer[] initializeZeroLengthList() {
    Integer[] zeroLengthList = new Integer[1];
    zeroLengthList[0] = 0;
    return zeroLengthList;
  }

  public Int32IterableCursor(String name, Integer[] array) {
    super(name);
    this.array = array;
  }

  @Override
  public Object value() {
    throw new NotImplementedException();
    //return array[start];
  }

  @Override
  public Iterator<Integer> iterator() {
    return new I32CursorIterator(array, this.start);
  }

  @Override
  public RollableRecordSet<Integer> i32Iter() {
    return new RollableRecordSet<Integer>(this);
  }

}
