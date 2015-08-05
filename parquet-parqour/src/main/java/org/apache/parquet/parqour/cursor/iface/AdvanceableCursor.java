package org.apache.parquet.parqour.cursor.iface;

import org.apache.parquet.parqour.cursor.iterators.RollableFieldEntries;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.entrysets.FieldEntries;

/**
 * Created by sircodesalot on 6/19/15.
 */
public abstract class AdvanceableCursor extends CursorBase {
  protected int index = 0;

  public AdvanceableCursor(String name, int columnIndex) {
    super(name, columnIndex);
    this.index = 0;
  }

  public AdvanceableCursor advanceTo(int index) {
    this.index = index;
    return this;
  }
}
