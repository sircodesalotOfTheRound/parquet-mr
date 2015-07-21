package org.apache.parquet.parqour.ingest.recordsets.transforms;

/**
 * Created by sircodesalot on 7/5/15.
 */
public interface FieldEntryReducerTransform<T, U> {
  U nextItem(U aggregate, T item);
}
