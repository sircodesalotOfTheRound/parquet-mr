package org.apache.parquet.parqour.exceptions;

/**
 * An Exception thrown when trying to build the filter-predicate.
 */
public class ColumnPredicateBuilderException extends ParqourException {
  public ColumnPredicateBuilderException(String format, Object... args) {
    super(format, args);
  }
}
