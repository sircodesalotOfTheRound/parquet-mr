package org.apache.parquet.parqour.ingest.cursor.iface;

import org.apache.parquet.parqour.ingest.entrysets.FieldEntries;
import org.apache.parquet.parqour.ingest.cursor.iterators.RollableFieldEntries;

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

  RollableFieldEntries<Integer> i32Iter(int columnIndex);
  RollableFieldEntries<Integer> i32Iter(String path);

  FieldEntries<Cursor> fieldIter();
  FieldEntries<Cursor> fieldIter(int columnIndex);
  FieldEntries<Cursor> fieldIter(String path);
}
