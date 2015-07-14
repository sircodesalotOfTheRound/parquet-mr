package org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.types.ColumnPredicateType;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;

/**
 * Created by sircodesalot on 6/2/15.
 */
public final class EqualsColumnPredicate extends ColumnPredicate.SystemDefinedPredicate {
  private final IngestTree ingestTree;

  public EqualsColumnPredicate(IngestTree ingestTree, ColumnPredicate parent, ColumnDescriptor column, Comparable value) {
    super(parent, ColumnPredicateType.EQ, column, value);
    this.ingestTree = ingestTree;
  }

  @Override
  public boolean test(Comparable comparable) {
    Object entry = ingestTree.root().collectAggregate().i32(columnPathString());
    return entry.equals(value());
  }
}
