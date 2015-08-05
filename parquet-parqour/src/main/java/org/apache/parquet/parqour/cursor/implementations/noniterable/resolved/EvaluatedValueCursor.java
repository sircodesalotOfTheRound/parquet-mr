package org.apache.parquet.parqour.cursor.implementations.noniterable.resolved;

import org.apache.parquet.parqour.cursor.iface.CursorBase;

/**
 * Created by sircodesalot on 8/5/15.
 */
//
public abstract class EvaluatedValueCursor extends CursorBase {
  public EvaluatedValueCursor(String name, int columnIndex) {
    super(name, columnIndex);
  }

}
