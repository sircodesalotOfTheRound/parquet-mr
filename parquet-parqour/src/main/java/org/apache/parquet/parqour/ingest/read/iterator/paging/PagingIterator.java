package org.apache.parquet.parqour.ingest.read.iterator.paging;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

/**
 * Created by sircodesalot on 6/27/15.
 */
public class PagingIterator<T> implements Iterator<ParqourPageset<T>> {
  private final Iterator<T> iterator;
  private final int size;


  public PagingIterator(Iterator<T> iterator, int size) {
    this.iterator = iterator;
    this.size = size;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ParqourPageset<T> next() {
    return new ParqourPageset<T>(iterator, size);
  }

  @Override
  public void remove() {
    throw new NotImplementedException();
  }
}
