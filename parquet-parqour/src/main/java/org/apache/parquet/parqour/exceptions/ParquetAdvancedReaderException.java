package org.apache.parquet.parqour.exceptions;

/**
 * Created by sircodesalot on 6/9/15.
 */
public class ParquetAdvancedReaderException extends RuntimeException {
  public ParquetAdvancedReaderException(String format, Object ... args) {
    super(String.format(format, args));
  }
}
