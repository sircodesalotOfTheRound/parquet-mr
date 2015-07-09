package org.apache.parquet.parqour.ingest.plan.predicates.builders.leaf.sys;

import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.ColumnPredicateBuildable;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys.GreaterThanColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys.LessThanOrEqualsColumnPredicate;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.column.ColumnDescriptor;

/**
* Created by sircodesalot on 6/2/15.
*/
public class GreaterThanColumnPredicateBuilder extends ColumnPredicateBuildable.SystemDefinedPredicateBuilder {
  public GreaterThanColumnPredicateBuilder(ColumnDescriptor column, Comparable value) {
    super(column, value);
  }

  @Override
  public ColumnPredicate build(ColumnPredicate parent, IngestTree ingestTree) {
    if (!isNegated()) {
      return new GreaterThanColumnPredicate(parent, this.column(), this.value());
    } else {
      return new LessThanOrEqualsColumnPredicate(parent, this.column(), this.value());
    }
  }
}
