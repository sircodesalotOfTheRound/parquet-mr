package org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys;

import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.types.ColumnPredicateType;
import org.apache.parquet.column.ColumnDescriptor;

/**
 * Created by sircodesalot on 6/2/15.
 */
public class GreaterThanColumnPredicate extends ColumnPredicate.SystemDefinedPredicate {
  public GreaterThanColumnPredicate(ColumnPredicate parent, ColumnDescriptor column, Comparable value) {
    super(parent, ColumnPredicateType.GT, column, value);
  }

  @Override
  public boolean test(Comparable entry) {
    return entry.compareTo(value()) > 0;
  }
}
