package org.apache.parquet.parqour.ingest.plan.predicates.leaf.udf;

import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.predicate.Operators;

/**
 * Created by sircodesalot on 6/2/15.
 */
public class NegatedUserDefinedColumnPredicate extends ColumnPredicate.UserDefinedColumnPredicateBase {
  public NegatedUserDefinedColumnPredicate(ColumnPredicate parent, ColumnDescriptor column, Operators.UserDefined function) {
    super(parent, column, function);
  }

  @Override
  public boolean test(Comparable entry) {
    return false;
  }
}

