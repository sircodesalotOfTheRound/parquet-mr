package org.apache.parquet.parqour.query.udf;

import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;

/**
 * Created by sircodesalot on 7/15/15.
 */
public abstract class TwoParameterUdf<T> extends ParqourUdf {
  public TwoParameterUdf() {
    super(UdfParameterLength.TWO);
  }

  public abstract T apply(Cursor lhs, Cursor rhs);
}
