package org.apache.parquet.parqour.ingest.read.driver;


import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;

/**
 * Created by sircodesalot on 6/9/15.
 */
public class ParquetAdvancedReader {
  private final SchemaInfo schemaInfo;
  private final ParqourReadDriver driver;

  public ParquetAdvancedReader(Configuration configuration, Path path, ParquetMetadata metadata, FilterPredicate predicate) {
    this(configuration, path, metadata, metadata.getFileMetaData().getSchema(), predicate);
  }

  public ParquetAdvancedReader(Configuration configuration, Path path, ParquetMetadata metadata,
                                    GroupType projectionSchema, FilterPredicate predicate) {

    this.schemaInfo = new SchemaInfo(configuration, path, metadata, projectionSchema, predicate);
    this.driver = new ParqourReadDriver(schemaInfo);
  }

  public SchemaInfo schemaInfo() { return this.schemaInfo; }

  public Object readRow() {

    return null;
  }

  public void readRowVector(int rowCount) {
    for (int count = 0; count < rowCount; count++) {
      readRow();
    }
  }
}
