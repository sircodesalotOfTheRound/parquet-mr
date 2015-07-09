package org.apache.parquet.parqour.ingest.plan.predicates.builders.logic;

import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.ColumnPredicateBuildable;
import org.apache.parquet.parqour.ingest.plan.predicates.logic.AndColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.logic.OrColumnPredicate;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;

/**
 * Created by sircodesalot on 6/2/15.
 */
public class AndColumnPredicateBuilder extends ColumnPredicateBuildable.LogicColumnPredicateBuilder {
  public AndColumnPredicateBuilder(ColumnPredicateBuildable lhs, ColumnPredicateBuildable rhs) {
    super(lhs, rhs);
  }

  @Override
  public ColumnPredicate build(ColumnPredicate parent, IngestTree ingestTree) {
    if (!isNegated()) {
      return new AndColumnPredicate(ingestTree, parent, this.lhs(), this.rhs());
    } else {
      // DeMorganize.
      this.lhs().negate();
      this. rhs().negate();
      return new OrColumnPredicate(ingestTree, parent, this.lhs(), this.rhs());
    }
  }
}
