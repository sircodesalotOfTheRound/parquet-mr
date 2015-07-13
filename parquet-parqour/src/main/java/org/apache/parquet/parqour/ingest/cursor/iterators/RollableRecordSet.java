package org.apache.parquet.parqour.ingest.cursor.iterators;

import org.apache.parquet.parqour.ingest.cursor.collections.Roll;

import java.util.Iterator;

/**
 * Created by sircodesalot on 7/5/15.
 */
public class RollableRecordSet<T> extends RecordSet<T> {
  public static RollableRecordSet<Integer> EMPTY_I32 = new RollableRecordSet<Integer>();

  protected RollableRecordSet() { }

  public RollableRecordSet(Iterable<T> iterable) {
    super(iterable);
  }

  public final Iterable<T> roll() {
    return new Roll<T>(this);
  }
}
