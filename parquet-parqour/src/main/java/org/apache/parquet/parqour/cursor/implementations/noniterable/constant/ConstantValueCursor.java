package org.apache.parquet.parqour.cursor.implementations.noniterable.constant;

import org.apache.parquet.parqour.cursor.iface.AdvanceableCursor;

/**
 * Created by sircodesalot on 8/4/15.
 */
public class ConstantValueCursor extends AdvanceableCursor {
  private final Object value;

  public ConstantValueCursor(String name, int columnIndex, Object value) {
    super(name, columnIndex);
    this.value = value;
  }

  @Override
  public Object value() {
    return this.value;
  }
}
