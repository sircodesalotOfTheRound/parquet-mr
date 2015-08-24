package org.apache.parquet.parqour.query.expressions.predicate.logical;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;

/**
 * Created by sircodesalot on 7/27/15.
 */
public class TextQueryLogicalOrExpression extends TextQueryLogicalExpression {
  public TextQueryLogicalOrExpression(TextQueryVariableExpression lhs, TextQueryVariableExpression rhs) {
    super(lhs, rhs, TextQueryExpressionType.OR);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

  @Override
  public TransformCollection<String> collectColumnDependencies(TransformList<String> collectTo) {
    return null;
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    return new TextQueryLogicalOrExpression(lhs().simplify(parent), rhs().simplify(parent));
  }

  @Override
  public TextQueryVariableExpression negate() {
    return new TextQueryLogicalAndExpression(lhs().negate(), rhs().negate());
  }

  @Override
  public InfixOperator operator() {
    return InfixOperator.OR;
  }
}
