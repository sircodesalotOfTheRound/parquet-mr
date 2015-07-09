package org.apache.parquet.parqour.exceptions;

/**
 * Created by sircodesalot on 6/2/15.
 */
public class ReadNodeException extends ParquetAdvancedReaderException {
  public ReadNodeException(String format, Object ... args) {
    super(format, args);
  }
}
