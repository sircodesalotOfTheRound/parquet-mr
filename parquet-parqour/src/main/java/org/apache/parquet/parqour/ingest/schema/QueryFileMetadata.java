package org.apache.parquet.parqour.ingest.schema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.parqour.exceptions.DataIngestException;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;


/**
 * Created by sircodesalot on 7/30/15.
 */
public class QueryFileMetadata {
  private final Configuration EMPTY_CONFIGURATION = new Configuration();
  private final QueryFilePath file;
  private final ParquetMetadata metadata;

  public QueryFileMetadata(QueryFilePath file) {
    this.file = file;
    this.metadata = captureMetadata(file);
  }

  private ParquetMetadata captureMetadata(QueryFilePath file) {
    try {
      Path path = new Path(file.path());
      return ParquetFileReader.readFooter(EMPTY_CONFIGURATION, path, ParquetMetadataConverter.NO_FILTER);
    } catch (IOException ex) {
      throw new DataIngestException("Unable to load parquet file");
    }
  }

  public String path() { return this.file.path(); }
  public MessageType baseSchema() { return metadata.getFileMetaData().getSchema(); }
  public String createdBy() { return metadata.getFileMetaData().getCreatedBy(); }
}
