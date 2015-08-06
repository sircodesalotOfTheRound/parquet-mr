package org.apache.parquet.parqour.query.expressions.predicate.testable;

import org.apache.parquet.parqour.query.expressions.categories.TextQueryExpressionType;
import org.apache.parquet.parqour.query.expressions.categories.TextQueryVariableExpression;
import org.apache.parquet.parqour.query.expressions.variable.infix.InfixOperator;
import org.apache.parquet.parqour.query.expressions.variable.infix.TextQueryInfixExpression;
import org.apache.parquet.parqour.query.visitor.TextQueryExpressionVisitor;

import java.util.regex.Pattern;

/**
 * Created by sircodesalot on 8/6/15.
 */
public class TextQueryMatchesExpression extends TextQueryTestableBinaryExpression<String> {
  private final Pattern pattern;
  private boolean isNegated = false;

  public TextQueryMatchesExpression(TextQueryInfixExpression infixExpression) {
    super(infixExpression, TextQueryExpressionType.MATCHES);

    this.pattern = Pattern.compile(lastSeenRhs);
  }

  @Override
  public InfixOperator operator() {
    return InfixOperator.MATCHES;
  }

  @Override
  public boolean test() {
    if (!lhsIsCached) {
      lastSeenLhs = (String)lhsCursor.value();
    }

    return pattern.matcher(lastSeenLhs).matches();
  }

  @Override
  public TextQueryVariableExpression negate() {
    this.isNegated = !isNegated;
    return this;
  }

  @Override
  public <TReturnType> TReturnType accept(TextQueryExpressionVisitor<TReturnType> visitor) {
    return null;
  }
}
