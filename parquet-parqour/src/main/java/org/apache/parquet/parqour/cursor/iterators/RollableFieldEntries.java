package org.apache.parquet.parqour.cursor.iterators;

import org.apache.parquet.parqour.ingest.entrysets.collections.Roll;
import org.apache.parquet.parqour.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.entrysets.FieldEntries;

/**
 * Created by sircodesalot on 7/5/15.
 */
public class RollableFieldEntries<T> extends FieldEntries<T> {
  public static RollableFieldEntries<Integer> EMPTY_I32_RECORDSET = new RollableFieldEntries<Integer>();
  public static RollableFieldEntries<Cursor> EMPTY_CURSOR_RECORDSET = new RollableFieldEntries<Cursor>();

  protected RollableFieldEntries() { }

  public RollableFieldEntries(Iterable<T> iterable) {
    super(iterable);
  }

  public final Iterable<T> roll() {
    return new Roll<T>(this);
  }
}
