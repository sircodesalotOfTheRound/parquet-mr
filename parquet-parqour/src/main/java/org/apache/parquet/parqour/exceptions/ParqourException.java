package org.apache.parquet.parqour.exceptions;

/**
 * Base class for all exceptions.
 */
public abstract class ParqourException extends RuntimeException {
  public ParqourException(String format, Object... args) {
    super(String.format(format, args));
  }
}
