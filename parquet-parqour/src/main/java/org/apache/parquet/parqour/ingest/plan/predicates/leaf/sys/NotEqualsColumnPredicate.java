package org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys;

import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.types.ColumnPredicateType;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.column.ColumnDescriptor;

/**
 * Created by sircodesalot on 6/2/15.
 */
public class NotEqualsColumnPredicate extends ColumnPredicate.SystemDefinedPredicate {
  public NotEqualsColumnPredicate(IngestTree ingestTree, ColumnPredicate parent, ColumnDescriptor column, Comparable value) {
    super(parent, ColumnPredicateType.NEQ, column, value);
  }

  @Override
  public boolean test(Comparable entry) {
    return !entry.equals(value());
  }
}
