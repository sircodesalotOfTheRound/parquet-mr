package org.apache.parquet.parqour.materialization;

import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.iterator.Parqour;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.hadoop.api.ReadSupport;

import java.util.Iterator;

/**
 * Created by sircodesalot on 7/4/15.
 */
public class ReadSupportIterable<T> extends Parqour<T> {
  private final ReadSupportMaterializer<T> materializer;
  private final Iterable<Cursor> iterable;

  public ReadSupportIterable(SchemaInfo schemaInfo, ReadSupport<T> readSupport, Iterable<Cursor> iterable) {
    this.materializer = new ReadSupportMaterializer<T>(schemaInfo, readSupport);
    this.iterable = iterable;
  }

  @Override
  public Iterator<T> iterator() {
    return new MaterializingIterator<T>(materializer, iterable.iterator());
  }
}
