package org.apache.parquet.parqour.query.expressions.predicate.testable;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 7/27/15.
 */
public class TextQueryTestableGreaterThanOrEqualsExpression extends TextQueryTestableBinaryExpression {
  public TextQueryTestableGreaterThanOrEqualsExpression(TextQueryInfixExpression infixExpression) {
    super(infixExpression, TextQueryExpressionType.GREATER_THAN_OR_EQUALS);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }

  @Override
  public TextQueryVariableExpression negate() {
    return new TextQueryTestableLessThanExpression(super.infixExpression);
  }

  @Override
  public void test() {

  }

  @Override
  public InfixOperator operator() { return InfixOperator.GREATER_THAN_OR_EQUALS; }
}
