package org.apache.parquet.parqour.ingest.cursor.implementations.noniterable.bool;

import org.apache.parquet.parqour.ingest.cursor.iface.AdvanceableCursor;

/**
 * Created by sircodesalot on 6/18/15.
 */
public final class BooleanCursor extends AdvanceableCursor {
  private Boolean[] array;

  public BooleanCursor(String name, int columnIndex, Boolean[] array) {
    super(name, columnIndex);
    this.array = array;
  }

  public void setArray(Boolean[] array) {
    this.array = array;
  }

  @Override
  public Boolean bool() {
    return array[index];
  }

  @Override
  public Object value() {
    return array[index];
  }
}
