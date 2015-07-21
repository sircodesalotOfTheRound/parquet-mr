package org.apache.parquet.parqour.ingest.entrysets.transforms;

import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.ingest.entrysets.FieldEntries;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

/**
 * Created by sircodesalot on 14-5-30.
 */
public class FieldEntryProjectionTransform<T, U> extends FieldEntries<U> {
  private final Projection<T, U> projection;
  private final Iterable<T> iterable;

  public FieldEntryProjectionTransform(Iterable<T> iterable, Projection<T, U> projection) {
    this.iterable = iterable;
    this.projection = projection;
  }

  @Override
  public Iterator<U> iterator() {
    final Iterator<T> iterator = this.iterable.iterator();

    // Return a projection iterator.
    return new Iterator<U>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public U next() {
        return projection.apply(iterator.next());
      }

      @Override
      public void remove() {
        throw new NotImplementedException();
      }
    };
  }
}
