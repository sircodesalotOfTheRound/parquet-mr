package org.apache.parquet.parqour.ingest.cursor.iterators;

import org.apache.parquet.parqour.ingest.cursor.collections.KeepList;

/**
 * Created by sircodesalot on 7/5/15.
 */
public class KeepableRecordSet<T> extends RecordSet<T> {
  public KeepableRecordSet(Iterable<T> iterable) {
    super(iterable);
  }

  public final Iterable<T> materialize() {
    return new KeepList<T>(this);
  }
}
