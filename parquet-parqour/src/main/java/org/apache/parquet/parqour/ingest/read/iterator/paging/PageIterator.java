package org.apache.parquet.parqour.ingest.read.iterator.paging;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

/**
 * Created by sircodesalot on 6/27/15.
 */
public class PageIterator<T> implements Iterator<T> {
  private final Iterator<T> iterator;
  private final int size;

  private int index;

  public PageIterator(Iterator<T> iterator, int size) {
    this.iterator = iterator;
    this.size = size;

    this.index = 0;
  }

  @Override
  public boolean hasNext() {
    return index < size && iterator.hasNext();
  }

  @Override
  public T next() {
    this.index += 1;
    return iterator.next();
  }

  @Override
  public void remove() {
    throw new NotImplementedException();
  }
}
