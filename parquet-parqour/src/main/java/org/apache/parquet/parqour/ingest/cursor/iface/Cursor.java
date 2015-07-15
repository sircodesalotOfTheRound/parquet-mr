package org.apache.parquet.parqour.ingest.cursor.iface;

import org.apache.parquet.parqour.ingest.cursor.iterators.RecordSet;
import org.apache.parquet.parqour.ingest.cursor.iterators.RollableRecordSet;

/**
 * Created by sircodesalot on 6/18/15.
 */
public interface Cursor {
  Integer i32();
  Integer i32(int columnIndex);
  Integer i32(String path);

  Object value();
  Object value(int columnIndex);
  Object value(String path);

  Cursor field(int columnIndex);
  Cursor field(String path);

  RollableRecordSet<Integer> i32Iter(int columnIndex);
  RollableRecordSet<Integer> i32Iter(String path);

  RecordSet<Cursor> fieldIter();
  RecordSet<Cursor> fieldIter(int columnIndex);
  RecordSet<Cursor> fieldIter(String path);
}
