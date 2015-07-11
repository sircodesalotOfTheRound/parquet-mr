package org.apache.parquet.parqour.ingest.cursor.iterators;

import org.apache.parquet.parqour.ingest.cursor.collections.Roll;

/**
 * Created by sircodesalot on 7/5/15.
 */
public class RollableRecordSet<T> extends RecordSet<T> {
  public RollableRecordSet(Iterable<T> iterable) {
    super(iterable);
  }

  public final Iterable<T> roll() {
    return new Roll<T>(this);
  }
}
