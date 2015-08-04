package org.apache.parquet.parqour.query.udf;

import org.apache.parquet.parqour.cursor.iface.Cursor;

/**
 * Created by sircodesalot on 7/15/15.
 */
public abstract class ThreeParameterUdf<T> extends ParqourUdf {
  public ThreeParameterUdf() {
    super(UdfParameterLength.THREE);
  }

  public abstract T apply(Cursor first, Cursor second, Cursor third);
}
