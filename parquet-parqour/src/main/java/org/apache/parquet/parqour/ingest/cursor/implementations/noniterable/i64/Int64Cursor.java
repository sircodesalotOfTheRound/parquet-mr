package org.apache.parquet.parqour.ingest.cursor.implementations.noniterable.i64;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;

/**
 * Created by sircodesalot on 6/18/15.
 */
public final class Int64Cursor extends AdvanceableCursor {
  private Long[] array;

  public Int64Cursor(String name, int columnIndex, Long[] array) {
    super(name, columnIndex);
    this.array = array;
  }

  public void setArray(Long[] array) {
    this.array = array;
  }

  @Override
  public Long i64() {
    return array[index];
  }

  @Override
  public Object value() {
    return array[index];
  }
}
