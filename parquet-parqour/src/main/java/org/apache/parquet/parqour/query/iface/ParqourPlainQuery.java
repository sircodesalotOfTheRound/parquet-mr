package org.apache.parquet.parqour.query.iface;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.iterator.ParqourRecordset;
import org.apache.parquet.parqour.ingest.schema.QueryInfo;
import org.apache.parquet.parqour.query.expressions.txql.TextQueryTreeRootExpression;

import java.util.Iterator;

/**
 * Created by sircodesalot on 7/5/15.
 */
public class ParqourPlainQuery extends ParqourQuery {
  public ParqourPlainQuery(ParqourSource source, TextQueryTreeRootExpression query) {
    super(source, query);
  }


  public ParquetMetadata metadata() { return this.metadata; }
  public QueryInfo schemaInfo() { return this.queryInfo; }

  @Override
  public Iterator<Cursor> iterator() {
    return new ParqourRecordset(queryInfo).iterator();
  }
}
