package org.apache.parquet.parqour.ingest.read.iterator.filtering;


import org.apache.parquet.parqour.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.query.iface.ParqourQuery;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

/**
 * Created by sircodesalot on 14-5-30.
 */
public class ParqourQueryFilterIterable extends ParqourQuery {
  private final ParqourQuery iterable;
  private final Predicate<Cursor> predicate;

  public ParqourQueryFilterIterable(ParqourQuery queryIterable, Predicate<Cursor> predicate) {
    super (queryIterable);

    this.iterable = queryIterable;
    this.predicate = predicate;
  }

  @Override
  public Iterator<Cursor> iterator() {
    return new ParqourQueryFilterIterator(iterable, predicate);
  }

  public class ParqourQueryFilterIterator implements Iterator<Cursor> {
    private final Iterator<Cursor> iterator;
    private final Predicate<Cursor> predicate;
    private boolean hasUpdated = false;
    private Cursor nextItem;

    private ParqourQueryFilterIterator(Iterable<Cursor> iterable, Predicate<Cursor> predicate) {
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
    public Cursor next() {
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
