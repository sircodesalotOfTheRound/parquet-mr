package org.apache.parquet.parqour.ingest.plan.predicates.builders.logic;

import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.ColumnPredicateBuildable;
import org.apache.parquet.parqour.ingest.plan.predicates.logic.AndColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.logic.OrColumnPredicate;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;

/**
 * Created by sircodesalot on 6/2/15.
 */
public class OrColumnPredicateBuilder extends ColumnPredicateBuildable.LogicColumnPredicateBuilder {
  public OrColumnPredicateBuilder(ColumnPredicateBuildable lhs, ColumnPredicateBuildable rhs) {
    super(lhs, rhs);
  }

  @Override
  public ColumnPredicate build(ColumnPredicate parent, IngestTree ingestTree) {
    if (!isNegated()) {
      return new OrColumnPredicate(ingestTree, parent, this.lhs(), this.rhs());
    } else {
      // DeMorganize.
      this.lhs().negate();
      this. rhs().negate();
      return new AndColumnPredicate(ingestTree, parent, this.lhs(), this.rhs());
    }
  }
}
