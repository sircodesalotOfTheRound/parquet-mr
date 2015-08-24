package org.apache.parquet.parqour.query.expressions.predicate.testable;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;
import org.apache.parquet.parqour.tools.TransformCollection;
import org.apache.parquet.parqour.tools.TransformList;

/**
 * Created by sircodesalot on 7/27/15.
 */
public class TextQueryTestableEqualsExpression extends TextQueryTestableBinaryExpression<Object> {
  public TextQueryTestableEqualsExpression(TextQueryInfixExpression infixExpression) {
    super(infixExpression, TextQueryExpressionType.EQUALS);
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
  public TextQueryVariableExpression negate() {
    return new TextQueryTestableNotEqualsExpression(super.infixExpression);
  }

  @Override
  public boolean test() {
    if (!lhsIsCached) {
      lastSeenLhs = lhsCursor.value();
    }

    if (!rhsIsCached) {
      lastSeenRhs = rhsCursor.value();
    }

    if (lastSeenLhs != null && lastSeenRhs != null) {
      return lastSeenLhs.equals(lastSeenRhs);
    } else {
      return false;
    }
  }

  @Override
  public InfixOperator operator() { return InfixOperator.EQUALS; }
}
