package org.apache.parquet.parqour.query.iface;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.parqour.ingest.cursor.iface.Cursor;
import org.apache.parquet.parqour.ingest.read.iterator.Parqour;
import org.apache.parquet.parqour.ingest.read.iterator.filtering.ParqourQueryFilterIterable;
import org.apache.parquet.parqour.ingest.read.iterator.lamba.Predicate;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.parqour.materialization.readsupport.ReadSupportIterable;
import org.apache.parquet.parqour.query.expressions.pql.TextQueryTreeRootExpression;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

/**
 * Created by sircodesalot on 6/15/15.
 */
public abstract class ParqourQuery extends Parqour<Cursor> {
  public static final Configuration EMPTY_CONFIGURATION = new Configuration();

  protected final ParqourSource source;
  protected final SchemaInfo schemaInfo;
  protected final TextQueryTreeRootExpression query;
  protected final ParquetMetadata metadata;

  public ParqourQuery (ParqourQuery query) {
    this.source = query.source;
    this.schemaInfo = query.schemaInfo;
    this.query = query.query;
    this.metadata = query.metadata;
  }

  public ParqourQuery(ParqourSource source, TextQueryTreeRootExpression query) {
    this.source = source;
    this.metadata = captureMetadata(source);
    this.schemaInfo = captureSchemaInfo(source, metadata);
    this.query = query;
  }

  private ParquetMetadata captureMetadata(ParqourSource source) {
    Path sourceFile = new Path(source.path());
    try {
      return ParquetFileReader.readFooter(EMPTY_CONFIGURATION, sourceFile, ParquetMetadataConverter.NO_FILTER);
    } catch (IOException e) {
      throw new DataIngestException("Unable to read file metadata");
    }
  }

  private SchemaInfo captureSchemaInfo(ParqourSource source, ParquetMetadata metadata) {
    Path sourceFile = new Path(source.path());
    MessageType schema = metadata.getFileMetaData().getSchema();
    return new SchemaInfo(EMPTY_CONFIGURATION, sourceFile, metadata, schema);
  }

  public static ParqourQuery fromRootExpression(TextQueryTreeRootExpression expression) {
    ParqourSource source = new ParqourSource(expression);
    return new ParqourPlainQuery(source, expression);
  }

  public ParqourQuery filter(Predicate<Cursor> expression) {
    return new ParqourQueryFilterIterable(this, expression);
  }

  public <T> Parqour<T> materialize(ReadSupport<T> readSupport) {
    return new ReadSupportIterable<T>(schemaInfo, readSupport, this);
  }
}
