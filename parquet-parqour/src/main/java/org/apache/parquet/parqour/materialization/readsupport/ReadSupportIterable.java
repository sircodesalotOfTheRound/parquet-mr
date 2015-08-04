package org.apache.parquet.parqour.materialization.readsupport;

import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.parqour.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.iterator.Parqour;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;

import java.util.Iterator;

/**
 * Created by sircodesalot on 7/4/15.
 */
public class ReadSupportIterable<T> extends Parqour<T> {
  private final ReadSupportMaterializer<T> materializer;
  private final Iterable<Cursor> iterable;

  public ReadSupportIterable(QueryInfo queryInfo, ReadSupport<T> readSupport, Iterable<Cursor> iterable) {
    this.materializer = new ReadSupportMaterializer<T>(queryInfo, readSupport);
    this.iterable = iterable;
  }

  @Override
  public Iterator<T> iterator() {
    return new ReadSupportMaterializingIterator<T>(materializer, iterable.iterator());
  }
}
