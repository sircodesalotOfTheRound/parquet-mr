package org.apache.parquet.parqour.ingest.plan.predicates.leaf.udf;

import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.predicate.Operators;

/**
 * Created by sircodesalot on 6/2/15.
 */
public class UserDefinedColumnPredicate extends ColumnPredicate.UserDefinedColumnPredicateBase {
  public UserDefinedColumnPredicate(ColumnPredicate parent, ColumnDescriptor column, Operators.UserDefined function) {
    super(parent, column, function);
  }

  @Override
  public boolean test(Comparable entry) {
    return function().keep(entry);
  }
}
