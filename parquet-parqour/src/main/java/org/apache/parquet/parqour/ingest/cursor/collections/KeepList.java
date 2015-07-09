package org.apache.parquet.parqour.ingest.cursor.collections;

import org.apache.parquet.parqour.ingest.cursor.iterators.KeepableRecordSet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by sircodesalot on 7/5/15.
 */
public class KeepList<T> extends KeepableRecordSet<T> {
  private final List<T> items;

  public KeepList(Iterable<T> iterable) {
    super(iterable);

    List<T> items = new ArrayList<T>();
    for (T item : iterable) {
      items.add(item);
    }

    this.items = items;
  }

  @Override
  public Iterator<T> iterator() {
    return items.iterator();
  }
}
