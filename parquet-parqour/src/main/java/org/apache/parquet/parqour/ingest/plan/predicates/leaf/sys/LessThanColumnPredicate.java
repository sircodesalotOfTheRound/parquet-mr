package org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.types.ColumnPredicateType;

/**
 * Created by sircodesalot on 6/2/15.
 */
public class LessThanColumnPredicate extends ColumnPredicate.SystemDefinedPredicate {
  public LessThanColumnPredicate(ColumnPredicate parent, ColumnDescriptor column, Comparable value) {
    super(parent, ColumnPredicateType.LT, column, value);
  }

  @Override
  public boolean test(Comparable entry) {
    return entry.compareTo(value()) < 0;
  }
}
