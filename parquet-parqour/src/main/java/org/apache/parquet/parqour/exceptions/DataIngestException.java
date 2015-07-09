package org.apache.parquet.parqour.exceptions;

/**
 * Created by sircodesalot on 6/9/15.
 */
public class DataIngestException extends ParquetAdvancedReaderException {
  public DataIngestException(String format, Object... args) {
    super (format, args);
  }
}
