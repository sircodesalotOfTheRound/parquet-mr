package org.apache.parquet.parqour.ingest.plan.predicates.logic;

import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.ColumnPredicateBuildable;
import org.apache.parquet.parqour.ingest.plan.predicates.types.ColumnPredicateType;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;

/**
 * Created by sircodesalot on 6/2/15.
 */
public class OrColumnPredicate extends ColumnPredicate.LogicColumnPredicate {
  public OrColumnPredicate(IngestTree ingestTree, ColumnPredicate parent, ColumnPredicateBuildable lhs, ColumnPredicateBuildable rhs) {
    super(ingestTree, parent, ColumnPredicateType.OR, lhs, rhs);
  }
}
