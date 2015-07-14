package org.apache.parquet.parqour.ingest.plan.analysis;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.ColumnPredicateBuildable;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.leaf.sys.*;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.logic.AndColumnPredicateBuilder;
import org.apache.parquet.parqour.ingest.plan.predicates.builders.logic.OrColumnPredicateBuilder;
import org.apache.parquet.parqour.ingest.read.nodes.IngestTree;
import org.apache.parquet.parqour.ingest.schema.SchemaInfo;
import org.apache.parquet.schema.MessageType;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashMap;
import java.util.Map;

/**
* Created by sircodesalot on 6/8/15.
*/
class PredicateAnalysisVisitor implements FilterPredicate.Visitor<ColumnPredicateBuildable> {
  private final IngestTree ingestTree;
  private final SchemaInfo schemaInfo;

  public PredicateAnalysisVisitor(IngestTree ingestTree) {
    this.ingestTree = ingestTree;
    this.schemaInfo = ingestTree.schemaInfo();
  }

  private Map<String, ColumnDescriptor> captureColumnDescriptors(MessageType schema) {
    Map<String, ColumnDescriptor> columnDescriptors = new HashMap<String, ColumnDescriptor>();
    for (ColumnDescriptor descriptor : schema.getColumns()) {
      ColumnPath path = ColumnPath.get(descriptor.getPath());
      columnDescriptors.put(path.toDotString(), descriptor);
    }

    return columnDescriptors;
  }

  private ColumnDescriptor getColumnDescriptor(Operators.Column column) {
    String columnPath = column.getColumnPath().toDotString();
    return schemaInfo.getColumnDescriptorByPath(columnPath);
  }

  @Override
  public <T extends Comparable<T>> ColumnPredicateBuildable visit(Operators.Eq<T> equals) {
    return new EqualsColumnPredicateBuilder(getColumnDescriptor(equals.getColumn()), equals.getValue());
  }

  @Override
  public <T extends Comparable<T>> ColumnPredicateBuildable visit(Operators.NotEq<T> notEquals) {
    return new NotEqualsColumnPredicateBuilder(getColumnDescriptor(notEquals.getColumn()), notEquals.getValue());
  }

  @Override
  public <T extends Comparable<T>> ColumnPredicateBuildable visit(Operators.Lt<T> lessThan) {
    return new LessThanColumnPredicateBuilder(getColumnDescriptor(lessThan.getColumn()), lessThan.getValue());
  }

  @Override
  public <T extends Comparable<T>> ColumnPredicateBuildable visit(Operators.LtEq<T> lessThanOrEquals) {
    return new LessThanOrEqualsColumnPredicateBuilder(getColumnDescriptor(lessThanOrEquals.getColumn()),  lessThanOrEquals.getValue());
  }

  @Override
  public <T extends Comparable<T>> ColumnPredicateBuildable visit(Operators.Gt<T> greaterThan) {
    return new GreaterThanColumnPredicateBuilder(getColumnDescriptor(greaterThan.getColumn()), greaterThan.getValue());
  }

  @Override
  public <T extends Comparable<T>> ColumnPredicateBuildable visit(Operators.GtEq<T> greaterThanOrEquals) {
    return new GreaterThanOrEqualsColumnPredicateBuilder(getColumnDescriptor(greaterThanOrEquals.getColumn()),  greaterThanOrEquals.getValue());
  }

  @Override
  public ColumnPredicateBuildable visit(Operators.And and) {
    ColumnPredicateBuildable lhs = and.getLeft().accept(this);
    ColumnPredicateBuildable rhs = and.getRight().accept(this);

    return new AndColumnPredicateBuilder(lhs, rhs);
  }

  @Override
  public ColumnPredicateBuildable visit(Operators.Or or) {
    ColumnPredicateBuildable lhs = or.getLeft().accept(this);
    ColumnPredicateBuildable rhs = or.getRight().accept(this);

    return new OrColumnPredicateBuilder(lhs, rhs);
  }

  @Override
  public ColumnPredicateBuildable visit(Operators.Not not) {
    ColumnPredicateBuildable predicate = not.getPredicate().accept(this);
    predicate.negate();

    return predicate;
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> ColumnPredicateBuildable visit(Operators.UserDefined<T, U> udp) {
    return new ColumnPredicateBuildable.UserDefinedColumnPredicateBuilder(getColumnDescriptor(udp.getColumn()), udp);
  }

  @Override
  public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> ColumnPredicateBuildable visit(Operators.LogicalNotUserDefined<T, U> udp) {
    throw new NotImplementedException();
  }
}
