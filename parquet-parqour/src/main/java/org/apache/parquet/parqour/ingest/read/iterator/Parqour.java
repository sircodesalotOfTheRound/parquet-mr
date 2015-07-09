package org.apache.parquet.parqour.ingest.read.iterator;

import org.apache.parquet.parqour.ingest.read.iterator.filtering.ParqourFilterIterable;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Projection;
import org.apache.parquet.parqour.ingest.read.iterator.paging.PagingIterable;
import org.apache.parquet.parqour.materialization.LambdaMaterializer;
import org.apache.parquet.parqour.query.expressions.pql.ParquelTreeRootExpression;
import org.apache.parquet.parqour.query.iface.ParqourQuery;

/**
 * Created by sircodesalot on 6/27/15.
 */
public abstract class Parqour<T> implements Iterable<T> {
  protected Parqour() { }

  public static ParqourQuery query(String pql) {
    ParquelTreeRootExpression expression = ParquelTreeRootExpression.fromString(pql);
    return ParqourQuery.fromRootExpression(expression);
  }

  public <U> Parqour<U> materialize(Projection<T, U> projection) {
    return new LambdaMaterializer<T, U>(this, projection);
  }

  public Parqour<T> filter(Predicate<T> expression) {
    return new ParqourFilterIterable<T>(this, expression);
  }

  public PagingIterable<T> paginate(int size) {
    return new PagingIterable<T>(this, size);
  }
}
