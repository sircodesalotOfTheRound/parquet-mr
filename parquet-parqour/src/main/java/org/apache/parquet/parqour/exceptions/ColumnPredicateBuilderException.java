package org.apache.parquet.parqour.exceptions;

/**
 * Created by sircodesalot on 6/2/15.
 */
public class ColumnPredicateBuilderException extends ParquetAdvancedReaderException {
  public ColumnPredicateBuilderException(String format, Object... args) {
    super(format, args);
  }
}
