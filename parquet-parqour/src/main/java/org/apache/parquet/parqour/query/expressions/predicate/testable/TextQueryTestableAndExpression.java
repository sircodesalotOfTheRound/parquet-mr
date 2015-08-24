package org.apache.parquet.parqour.query.expressions.predicate.testable;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 8/24/15.
 */
public class TextQueryTestableAndExpression extends TextQueryTestableBinaryExpression<Object> {
  public TextQueryTestableAndExpression(TextQueryInfixExpression infixExpression, TextQueryExpressionType type) {
    super(infixExpression, type);
  }

  @Override
  public InfixOperator operator() {
    return InfixOperator.AND;
  }

  @Override
  public boolean test() {
    return false;
  }

  @Override
  public TextQueryVariableExpression negate() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }
}
