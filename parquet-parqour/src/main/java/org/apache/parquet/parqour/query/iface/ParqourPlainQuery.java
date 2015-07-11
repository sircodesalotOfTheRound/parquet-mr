package org.apache.parquet.parqour.query.iface;

import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.iterator.ParqourRecordset;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryTreeRootExpression;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.util.Iterator;

/**
 * Created by sircodesalot on 7/5/15.
 */
public class ParqourPlainQuery extends ParqourQuery {
  public ParqourPlainQuery(ParqourSource source, TextQueryTreeRootExpression query) {
    super(source, query);
  }


  public ParquetMetadata metadata() { return this.metadata; }
  public SchemaInfo schemaInfo() { return this.schemaInfo; }

  @Override
  public Iterator<Cursor> iterator() {
    return new ParqourRecordset(schemaInfo).iterator();
  }
}
