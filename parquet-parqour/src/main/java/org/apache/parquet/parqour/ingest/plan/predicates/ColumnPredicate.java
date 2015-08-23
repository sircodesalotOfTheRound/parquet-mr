package org.apache.parquet.parqour.ingest.plan.predicates;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.ColumnPredicateBuildable;
import org.apache.parquet.parqour.ingest.plan.predicates.traversal.TraversalInfo_OLD;
import org.apache.parquet.parqour.ingest.plan.predicates.types.ColumnPredicateNodeCategory;
import org.apache.parquet.parqour.ingest.plan.predicates.types.ColumnPredicateType;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;

/**
 * Created by sircodesalot on 6/2/15.
 */
public abstract class ColumnPredicate {
  public static TautologicalColumnPredicate NONE = TautologicalColumnPredicate.INSTANCE;

  private final ColumnPredicate parent;
  private final ColumnPredicateNodeCategory nodeCategory;
  private final ColumnPredicateType predicateType;

  public ColumnPredicate(ColumnPredicate parent, ColumnPredicateNodeCategory nodeType, ColumnPredicateType predicateType) {
    this.parent = parent;
    this.nodeCategory = nodeType;
    this.predicateType = predicateType;
  }

  public static abstract class LeafColumnPredicate extends ColumnPredicate {
    private final ColumnDescriptor column;
    private final ColumnPath columnPath;
    private final TraversalInfo_OLD traversalInfo;

    public LeafColumnPredicate(ColumnPredicate parent, ColumnDescriptor column,
                               ColumnPredicateNodeCategory nodeType, ColumnPredicateType predicateType) {
      super(parent, nodeType, predicateType);
      this.column = column;
      this.columnPath = ColumnPath.get(column.getPath());
      this.traversalInfo = new TraversalInfo_OLD(this);
    }

    public ColumnDescriptor column() { return this.column; }
    public String columnPathString() { return this.columnPath.toDotString(); }
    public ColumnPath columnPath() { return this.columnPath; }
    public abstract boolean test(Comparable entry);
    public TraversalInfo_OLD traversalInfo() { return this.traversalInfo; }
  }

  public static abstract class SystemDefinedPredicate extends LeafColumnPredicate {
    private final Comparable value;

    public SystemDefinedPredicate(ColumnPredicate parent, ColumnPredicateType predicateType,
                                  ColumnDescriptor column, Comparable value) {
      super(parent, column, ColumnPredicateNodeCategory.SYSTEM_DEFINED_LEAF, predicateType);
      this.value = value;
    }

    public Object value() { return this.value; }

    @Override
    public boolean test(Comparable entry) {
      return false;
    }
  }

  public static abstract class LogicColumnPredicate extends ColumnPredicate {
    private final ColumnPredicate lhs;
    private final ColumnPredicate rhs;
    private final TraversalInfo_OLD traversalInfo;

    public LogicColumnPredicate(IngestTree ingestTree, ColumnPredicate parent, ColumnPredicateType predicateType,
                                ColumnPredicateBuildable lhs, ColumnPredicateBuildable rhs) {
      super(parent, ColumnPredicateNodeCategory.LOGIC, predicateType);
      this.lhs = lhs.build(this, ingestTree);
      this.rhs = rhs.build(this, ingestTree);
      this.traversalInfo = new TraversalInfo_OLD(this);
    }

    public ColumnPredicate lhs() { return this.lhs; }
    public ColumnPredicate rhs() { return this.rhs; }
    public TraversalInfo_OLD traversalInfo() { return this.traversalInfo; }
  }

  public static abstract class UserDefinedColumnPredicateBase extends LeafColumnPredicate {
    private final UserDefinedPredicate function;

    public UserDefinedColumnPredicateBase(ColumnPredicate parent, ColumnDescriptor column, Operators.UserDefined function) {
      super(parent, column, ColumnPredicateNodeCategory.USER_DEFINED_LEAF, ColumnPredicateType.USER_DEFINED);
      this.function = function.getUserDefinedPredicate();
    }

    public UserDefinedPredicate function() {
      return function;
    }

    @Override
    public boolean test(Comparable entry) {
      return false;
    }
  }

  public ColumnPredicateNodeCategory nodeCategory() { return this.nodeCategory; }
  public ColumnPredicateType predicateType() { return this.predicateType; }
  public abstract TraversalInfo_OLD traversalInfo();
  public ColumnPredicate parent() { return this.parent; }
  public boolean hasParent() { return this.parent != null; }
}
