package org.apache.parquet.parqour.query.udf;

import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;

/**
 * Created by sircodesalot on 7/15/15.
 */
public abstract class SingleParameterUdf<T> extends ParqourUdf {
  public SingleParameterUdf() {
    super(UdfParameterLength.ONE);
  }

  public abstract T apply(Cursor cursor);
}
