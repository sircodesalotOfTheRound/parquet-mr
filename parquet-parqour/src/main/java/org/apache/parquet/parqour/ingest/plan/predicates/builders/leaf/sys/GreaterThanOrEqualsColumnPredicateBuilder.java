package org.apache.parquet.parqour.ingest.plan.predicates.builders.leaf.sys;

import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.ColumnPredicateBuildable;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys.GreaterThanOrEqualsColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys.LessThanColumnPredicate;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.column.ColumnDescriptor;

/**
* Created by sircodesalot on 6/2/15.
*/
public class GreaterThanOrEqualsColumnPredicateBuilder extends ColumnPredicateBuildable.SystemDefinedPredicateBuilder {
  public GreaterThanOrEqualsColumnPredicateBuilder(ColumnDescriptor column, Comparable value) {
    super(column, value);
  }

  @Override
  public ColumnPredicate build(ColumnPredicate parent, IngestTree ingestTree) {
    if (!isNegated()) {
      return new GreaterThanOrEqualsColumnPredicate(parent, this.column(), this.value());
    } else {
      return new LessThanColumnPredicate(parent, this.column(), this.value());
    }
  }
}
