package org.apache.parquet.parqour.ingest.read.iterator.filtering;


import org.apache.parquet.parqour.ingest.read.iterator.Parqour;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

/**
 * Created by sircodesalot on 14-5-30.
 */
public class ParqourFilterIterable<T> extends Parqour<T> {
  private final Parqour<T> iterable;
  private final Predicate<T> predicate;

  public ParqourFilterIterable(Parqour<T> iterable, Predicate<T> predicate) {
    this.iterable = iterable;
    this.predicate = predicate;
  }

  @Override
  public Iterator<T> iterator() {
    return new ParqourFilterIterator(iterable, predicate);
  }

  public class ParqourFilterIterator implements Iterator<T> {
    private final Iterator<T> iterator;
    private final Predicate<T> predicate;
    private boolean hasUpdated = false;
    private T nextItem;

    private ParqourFilterIterator(Iterable<T> iterable, Predicate<T> predicate) {
      this.iterator = iterable.iterator();
      this.predicate = predicate;
    }

    @Override
    public boolean hasNext() {
      hasUpdated = true;
      while (this.iterator.hasNext()) {
        this.nextItem = iterator.next();
        if (predicate.test(nextItem)) return true;
      }

      nextItem = null;
      return false;
    }

    @Override
    public T next() {
      if (!hasUpdated) {
        this.hasNext();
      }

      hasUpdated = false;
      return nextItem;
    }

    @Override
    public void remove() {
      throw new NotImplementedException();
    }
  }
}
