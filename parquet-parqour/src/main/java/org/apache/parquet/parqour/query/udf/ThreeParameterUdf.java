package org.apache.parquet.parqour.query.udf;

import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;

/**
 * Created by sircodesalot on 7/15/15.
 */
public interface ThreeParameterUdf<T> extends ParqourUdf {
  T apply(Cursor first, Cursor second, Cursor third);
}
