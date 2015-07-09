package org.apache.parquet.parqour.ingest.read.iterator.paging;

import org.apache.parquet.parqour.ingest.read.iterator.Parqour;

import java.util.Iterator;

/**
 * Created by sircodesalot on 6/27/15.
 */
public class ParqourPageset<T> extends Parqour<T> {
  private final Iterator<T> iterator;
  private final int size;

  public ParqourPageset(Iterator<T> iterator, int size) {
    this.iterator = iterator;
    this.size = size;
  }

  @Override
  public Iterator<T> iterator() {
    return new PageIterator<T>(iterator, size);
  }
}
