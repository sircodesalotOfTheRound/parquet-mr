package org.apache.parquet.parqour.materialization.readsupport;

import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

/**
 * Created by sircodesalot on 7/4/15.
 */
public class ReadSupportMaterializingIterator<T> implements Iterator<T> {
  private final Iterator<Cursor> iterator;
  private final ReadSupportMaterializer<T> materializer;

  public ReadSupportMaterializingIterator(ReadSupportMaterializer<T> materializer, Iterator<Cursor> iterator) {
    this.materializer = materializer;
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public T next() {
    return materializer.materializeRecord(iterator.next());
  }

  @Override
  public void remove() {
    throw new NotImplementedException();
  }
}
