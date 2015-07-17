package org.apache.parquet.parqour.ingest.cursor.iterators;

/**
 * Created by sircodesalot on 7/5/15.
 */
public interface Reducer<T, U> {
  void nextItem(U aggregate, T item);
}
