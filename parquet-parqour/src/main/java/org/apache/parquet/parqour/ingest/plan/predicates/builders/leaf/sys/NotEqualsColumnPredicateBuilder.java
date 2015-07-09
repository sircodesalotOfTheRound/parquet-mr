package org.apache.parquet.parqour.ingest.plan.predicates.builders.leaf.sys;

import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.ColumnPredicateBuildable;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys.EqualsColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.sys.NotEqualsColumnPredicate;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.column.ColumnDescriptor;

/**
* Created by sircodesalot on 6/2/15.
*/
public class NotEqualsColumnPredicateBuilder extends ColumnPredicateBuildable.SystemDefinedPredicateBuilder {
  public NotEqualsColumnPredicateBuilder(ColumnDescriptor column, Comparable value) {
    super(column, value);
  }

  @Override
  public ColumnPredicate build(ColumnPredicate parent, IngestTree ingestTree) {
    if (!isNegated()) {
      return new NotEqualsColumnPredicate(ingestTree, parent, this.column(), this.value());
    } else {
      return new EqualsColumnPredicate(ingestTree, parent, this.column(), this.value());
    }
  }
}
