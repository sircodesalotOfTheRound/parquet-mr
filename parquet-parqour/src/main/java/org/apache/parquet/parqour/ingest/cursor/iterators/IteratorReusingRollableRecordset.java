package org.apache.parquet.parqour.ingest.cursor.iterators;

import java.util.Iterator;

/**
 * Created by sircodesalot on 7/13/15.
 */

@Deprecated
public class IteratorReusingRollableRecordset<T> extends RollableRecordSet<T> {
  public IteratorReusingRollableRecordset(Iterable<T> iterable) {
  }

  @Override
  public Iterator<T> iterator() {
    return super.iterator();
  }
}
