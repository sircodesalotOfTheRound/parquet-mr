package org.apache.parquet.parqour.ingest.read.iterator;

import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.driver.ParqourReadDriverBase;
import org.apache.parquet.parqour.ingest.read.iterator.filtering.ParqourFilterIterable;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.parqour.materialization.lambda.LambdaMaterializer;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

/**
 * Created by sircodesalot on 6/27/15.
 */
public class ParqourRecordset extends Parqour<Cursor> {
  public static class ParqourCursorIterator implements Iterator<Cursor> {
    private final QueryInfo queryInfo;
    private final ParqourReadDriverBase driver;
    private final Cursor cursor;
    private boolean itemAvailable;

    public ParqourCursorIterator(QueryInfo queryInfo) {
      this.queryInfo = queryInfo;
      this.driver = ParqourReadDriverBase.determineReadDriverFromSchemaInfo(queryInfo);
      this.cursor = driver.cursor();

      this.itemAvailable = false;
    }

    @Override
    public boolean hasNext() {
      if (!itemAvailable) {
        itemAvailable = driver.readNext();
      }

      return itemAvailable;
    }

    @Override
    public Cursor next() {
      if (!itemAvailable) {
        throw new DataIngestException("Read past end of file");
      } else {
        itemAvailable = false;
        return cursor;
      }
    }

    @Override
    public void remove() {
      throw new NotImplementedException();
    }
  }

  private final QueryInfo queryInfo;

  public ParqourRecordset(QueryInfo queryInfo) {
    this.queryInfo = queryInfo;
  }

  public Parqour<Cursor> filter(Predicate<Cursor> expression) {
    return new ParqourFilterIterable<Cursor>(this, expression);
  }

  public <T> Parqour<T> materialize(Projection<Cursor, T> projection) {
    return new LambdaMaterializer<Cursor, T>(this, projection);
  }


  @Override
  public Iterator<Cursor> iterator() {
    return new ParqourCursorIterator(queryInfo);
  }
}
