package org.apache.parquet.parqour.ingest.cursor.iterators;


import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

/**
 * Created by sircodesalot on 14-5-30.
 */
public class RecordsetFilterIterable<T> extends RecordSet<T> {
  private final Predicate<T> predicate;

  public RecordsetFilterIterable(RecordSet<T> recordSet, Predicate<T> predicate) {
    super(recordSet);
    this.predicate = predicate;
  }

  @Override
  public Iterator<T> iterator() {
    return new RecordSetFilterIterator(this, predicate);
  }

  public class RecordSetFilterIterator implements Iterator<T> {
    private final Iterator<T> iterator;
    private final Predicate<T> predicate;
    private boolean hasUpdated = false;
    private T nextItem;

    private RecordSetFilterIterator(Iterable<T> iterable, Predicate<T> predicate) {
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
