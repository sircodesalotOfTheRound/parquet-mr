package org.apache.parquet.parqour.ingest.read.iterator.paging;

import org.apache.parquet.parqour.ingest.read.iterator.Parqour;

import java.util.Iterator;

/**
 * Created by sircodesalot on 6/27/15.
 */
public class PagingIterable<T> implements Iterable<ParqourPageset<T>> {
  private final int size;
  private final Parqour<T> recordset;

  public PagingIterable(Parqour<T> recordset, int size) {
    this.recordset = recordset;
    this.size = size;
  }
  @Override
  public Iterator<ParqourPageset<T>> iterator() {
    return new PagingIterator<T>(recordset.iterator(), size);
  }
}
