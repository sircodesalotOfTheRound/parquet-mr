package org.apache.parquet.parqour.ingest.cursor;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

/**
 * Created by sircodesalot on 6/18/15.
 */
public final class Int32Cursor extends AdvanceableCursor {
  private Integer[] array;

  public Int32Cursor(String name, int columnIndex, Integer[] array) {
    super(name, columnIndex);
    this.array = array;
  }

  public void setArray(Integer[] array) {
    this.array = array;
  }

  @Override
  public Integer i32() {
    return array[index];
  }

  @Override
  public Object value() {
    return array[index];
  }
}
