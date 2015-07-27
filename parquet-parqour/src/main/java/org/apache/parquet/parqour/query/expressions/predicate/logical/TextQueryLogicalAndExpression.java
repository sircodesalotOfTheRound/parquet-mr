package org.apache.parquet.parqour.query.expressions.predicate.logical;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 7/27/15.
 */
public class TextQueryLogicalAndExpression extends TextQueryLogicalExpression {
  public TextQueryLogicalAndExpression(TextQueryVariableExpression lhs, TextQueryVariableExpression rhs) {
    super(lhs, rhs, TextQueryExpressionType.AND);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    return new TextQueryLogicalAndExpression(lhs().simplify(parent), rhs().simplify(parent));
  }

  @Override
  public TextQueryVariableExpression negate() {
    return new TextQueryLogicalOrExpression(lhs().negate(), rhs().negate());
  }

  @Override
  public InfixOperator operator() {
    return InfixOperator.AND;
  }
}
