package org.apache.parquet.parqour.ingest.read.iterator.paging;

import org.apache.parquet.parqour.ingest.read.iterator.Parqour;

import java.util.Iterator;

/**
 * Created by sircodesalot on 6/27/15.
 */
public class ParqourPageset<T> implements Iterable<ParqourPage<T>> {
  private final int size;
  private final Parqour<T> recordset;

  public ParqourPageset(Parqour<T> recordset, int size) {
    this.recordset = recordset;
    this.size = size;
  }
  @Override
  public Iterator<ParqourPage<T>> iterator() {
    return new PagingIterator<T>(recordset.iterator(), size);
  }
}
