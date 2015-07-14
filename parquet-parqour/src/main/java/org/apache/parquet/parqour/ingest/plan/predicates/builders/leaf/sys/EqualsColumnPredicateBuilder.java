package org.apache.parquet.parqour.ingest.plan.predicates.builders.leaf.sys;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.ColumnPredicateBuildable;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys.EqualsColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys.NotEqualsColumnPredicate;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;

/**
* Created by sircodesalot on 6/2/15.
*/
public class EqualsColumnPredicateBuilder extends ColumnPredicateBuildable.SystemDefinedPredicateBuilder {
  public EqualsColumnPredicateBuilder(ColumnDescriptor column, Comparable value) {
    super(column, value);
  }

  @Override
  public ColumnPredicate build(ColumnPredicate parent, IngestTree ingestTree) {
    if (!isNegated()) {
      return new EqualsColumnPredicate(ingestTree, parent, this.column(), this.value());
    } else {
      return new NotEqualsColumnPredicate(ingestTree, parent, this.column(), this.value());
    }
  }
}
