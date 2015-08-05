package org.apache.parquet.parqour.query.expressions.predicate.testable;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

/**
 * Created by sircodesalot on 7/27/15.
 */
public class TextQueryTestableNotEqualsExpression extends TextQueryTestableBinaryExpression {
  public TextQueryTestableNotEqualsExpression(TextQueryInfixExpression infixExpression) {
    super(infixExpression, TextQueryExpressionType.NOT_EQUALS);
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;

  }

  @Override
  public boolean test() {
    if (!lhsIsCached) {
      lastSeenLhs = (Comparable) lhsCursor.value();
    }

    if (!rhsIsCached) {
      lastSeenRhs = (Comparable) rhsCursor.value();
    }

    if (lastSeenLhs != null && lastSeenRhs != null) {
      return !lastSeenLhs.equals(lastSeenRhs);
    } else {
      return false;
    }
  }

  @Override
  public TextQueryVariableExpression negate() {
    return new TextQueryTestableEqualsExpression(super.infixExpression);
  }

  @Override
  public InfixOperator operator() { return InfixOperator.NOT_EQUALS; }
}
