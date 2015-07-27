package org.apache.parquet.parqour.query.expressions.predicate.testable;

import org.apache.parquet.parqour.query.expressions.TextQueryExpression;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.predicate.TextQueryTestablePredicateExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;

/**
 * Created by sircodesalot on 7/27/15.
 */
public abstract class TextQueryTestableBinaryExpression extends TextQueryTestablePredicateExpression {
  protected final TextQueryInfixExpression infixExpression;

  public TextQueryTestableBinaryExpression(TextQueryInfixExpression infixExpression, TextQueryExpressionType type) {
    super(infixExpression.parent(), type);

    this.infixExpression = infixExpression;
  }

  @Override
  public TextQueryVariableExpression simplify(TextQueryExpression parent) {
    return infixExpression.simplify(parent);
  }


  public TextQueryVariableExpression lhs() { return infixExpression.lhs(); }
  public abstract InfixOperator operator();
  public TextQueryVariableExpression rhs() { return infixExpression.rhs(); }
}
