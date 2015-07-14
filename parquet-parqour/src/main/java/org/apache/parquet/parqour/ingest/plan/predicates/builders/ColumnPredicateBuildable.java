package org.apache.parquet.parqour.ingest.plan.predicates.builders;


import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.parqour.exceptions.ColumnPredicateBuilderException;
import org.apache.parquet.parqour.ingest.plan.predicates.ColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.udf.NegatedUserDefinedColumnPredicate;
import org.apache.parquet.parqour.ingest.plan.predicates.leaf.udf.UserDefinedColumnPredicate;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;

/**
 * Created by sircodesalot on 6/2/15.
 */
public abstract class ColumnPredicateBuildable {
  private boolean negated = false;

  public static abstract class LeafColumnPredicateBuilder extends ColumnPredicateBuildable {
    private ColumnDescriptor column;

    public LeafColumnPredicateBuilder(ColumnDescriptor column) {
      this.column = column;
    }

    public ColumnDescriptor column() {
      return this.column;
    }
  }

  public static abstract class SystemDefinedPredicateBuilder extends LeafColumnPredicateBuilder {
    private final Comparable value;

    public SystemDefinedPredicateBuilder(ColumnDescriptor column, Comparable value) {
      super(column);
      this.value = value;
    }
    public Comparable value() { return this.value; }
  }

  public static abstract class LogicColumnPredicateBuilder extends ColumnPredicateBuildable {
    private ColumnPredicateBuildable lhs;
    private ColumnPredicateBuildable rhs;

    public LogicColumnPredicateBuilder(ColumnPredicateBuildable lhs, ColumnPredicateBuildable rhs) {
      this.validate(lhs, rhs);

      this.lhs = lhs;
      this.rhs = rhs;
    }

    private void validate(ColumnPredicateBuildable lhs, ColumnPredicateBuildable rhs) {
      if (lhs == null || rhs == null) {
        throw new ColumnPredicateBuilderException("Children of class '%s' must not be null.", this.getClass().getName());
      }
    }

    public ColumnPredicateBuildable lhs() { return this.lhs; }
    public ColumnPredicateBuildable rhs() { return this.rhs; }
  }

  public static class UserDefinedColumnPredicateBuilder extends LeafColumnPredicateBuilder {
    private Operators.UserDefined userDefinedPredicate;
    public UserDefinedColumnPredicateBuilder(ColumnDescriptor column, Operators.UserDefined userDefinedPredicate) {
      super(column);
      this.userDefinedPredicate = userDefinedPredicate;
    }

    public Operators.UserDefined userDefinedPredicate() { return this.userDefinedPredicate; }

    @Override
    public ColumnPredicate build(ColumnPredicate parent, IngestTree ingestTree) {
      if (!isNegated()) {
        return new UserDefinedColumnPredicate(parent, this.column(), userDefinedPredicate);
      } else {
        return new NegatedUserDefinedColumnPredicate(parent, this.column(), userDefinedPredicate);
      }
    }
  }

  public void negate() {
    this.negated = !this.negated;
  }

  public boolean isNegated() {
    return this.negated;
  }

  public abstract ColumnPredicate build(ColumnPredicate parent, IngestTree ingestTree);
}
