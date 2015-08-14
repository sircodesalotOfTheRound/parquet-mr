package org.apache.parquet.parqour.exceptions;

/**
 * General Ingest Failure.
 */
public class DataIngestException extends ParqourException {
  public DataIngestException(String format, Object... args) {
    super (format, args);
  }

  public DataIngestException(Exception innerException, String format, Object... args) {
    super (innerException, format, args);
  }
}
