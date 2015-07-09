package org.apache.parquet.parqour.exceptions;

/**
 * Created by sircodesalot on 6/2/15.
 */
public class ParquelException extends ParquetAdvancedReaderException {
  public ParquelException(String format, Object... args) {
    super(format, args);
  }
}
