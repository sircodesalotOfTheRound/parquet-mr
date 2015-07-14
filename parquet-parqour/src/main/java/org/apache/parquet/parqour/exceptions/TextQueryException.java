package org.apache.parquet.parqour.exceptions;

/**
 * Thrown on text-query parser failure.
 */
public class TextQueryException extends ParqourException {
  public TextQueryException(String format, Object... args) {
    super(format, args);
  }
}
